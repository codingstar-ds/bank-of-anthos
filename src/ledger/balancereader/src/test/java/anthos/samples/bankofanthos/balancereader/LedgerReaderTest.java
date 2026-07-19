/*
 * Copyright 2020, Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package anthos.samples.bankofanthos.balancereader;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.web.client.ResourceAccessException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LedgerReader}.
 *
 * <p>The repository is mocked and the background polling thread is driven
 * deterministically: each polling test stubs {@code latestTransactionId()} so
 * that the loop terminates on its own (a remote id lower than the local id sets
 * {@code alive = false}). No live database or network access occurs.
 */
class LedgerReaderTest {

    private static final long STARTING_TRANSACTION_ID = -1;
    private static final long THREAD_JOIN_TIMEOUT_MS = 5000;

    private LedgerReader ledgerReader;
    private TransactionRepository dbRepo;
    private LedgerReaderCallback callback;

    @BeforeEach
    void setUp() {
        ledgerReader = new LedgerReader();
        dbRepo = mock(TransactionRepository.class);
        callback = mock(LedgerReaderCallback.class);
        setField(ledgerReader, "dbRepo", dbRepo);
        // Small poll interval keeps the thread-based tests fast.
        setField(ledgerReader, "pollMs", 5);
    }

    @Test
    @DisplayName("startWithCallback throws when the callback is null")
    void startWithCallbackThrowsWhenCallbackIsNull() {
        assertThrows(IllegalStateException.class,
            () -> ledgerReader.startWithCallback(null));
    }

    @Test
    @DisplayName("startWithCallback tolerates a database error during init")
    void startWithCallbackHandlesInitDatabaseError() throws Exception {
        // init call fails; first loop poll returns an id below the (unset) local
        // id so the background thread stops immediately.
        when(dbRepo.latestTransactionId())
            .thenThrow(new ResourceAccessException("db unreachable"))
            .thenReturn(STARTING_TRANSACTION_ID - 1);

        ledgerReader.startWithCallback(callback);
        joinBackgroundThread();

        // init failure leaves the local id at its starting value.
        assertEquals(STARTING_TRANSACTION_ID,
            getLongField(ledgerReader, "latestTransactionId"));
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("polling loop processes new transactions then stops when out of sync")
    void pollingLoopProcessesNewTransactionsThenStopsWhenOutOfSync()
        throws Exception {
        List<Transaction> newTransactions = Arrays.asList(
            transactionWithId(8L), transactionWithId(10L));
        when(dbRepo.findLatest(anyLong())).thenReturn(newTransactions);
        // init=5, ==5 (no change), db error (falls back to local), 10 (new
        // transactions), 3 (< local id 10 -> out of sync, stop).
        when(dbRepo.latestTransactionId())
            .thenReturn(5L, 5L)
            .thenThrow(new DataAccessResourceFailureException("blip"))
            .thenReturn(10L, 3L);

        ledgerReader.startWithCallback(callback);

        verify(callback, timeout(THREAD_JOIN_TIMEOUT_MS))
            .processTransaction(newTransactions.get(0));
        verify(callback, timeout(THREAD_JOIN_TIMEOUT_MS))
            .processTransaction(newTransactions.get(1));
        joinBackgroundThread();

        assertEquals(10L, getLongField(ledgerReader, "latestTransactionId"));
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("polling loop logs and continues when sleep is interrupted")
    void pollingLoopHandlesInterruptedSleep() throws Exception {
        setField(ledgerReader, "pollMs", 60000);
        when(dbRepo.latestTransactionId()).thenReturn(5L, 3L);

        ledgerReader.startWithCallback(callback);
        Thread background = getBackgroundThread();
        waitUntilSleeping(background);
        // Thread is running (sleeping) -> isAlive() reports true.
        assertTrue(ledgerReader.isAlive());
        // Interrupt the sleep: the loop should log a warning and continue, then
        // stop on the next poll (3 < 5).
        background.interrupt();
        joinBackgroundThread();

        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("isAlive is true before start and false after the thread stops")
    void isAliveReflectsBackgroundThreadState() throws Exception {
        // No background thread yet.
        assertTrue(ledgerReader.isAlive());

        when(dbRepo.latestTransactionId())
            .thenReturn(5L, STARTING_TRANSACTION_ID - 1);
        ledgerReader.startWithCallback(callback);
        joinBackgroundThread();

        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("pollTransactions returns the starting id when there are no new transactions")
    void pollTransactionsReturnsStartingIdWhenEmpty() throws Exception {
        setField(ledgerReader, "callback", callback);
        when(dbRepo.findLatest(anyLong())).thenReturn(Collections.emptyList());

        long result = invokePollTransactions(42L);

        assertEquals(42L, result);
        verify(callback, never()).processTransaction(any());
    }

    @Test
    @DisplayName("pollTransactions processes each transaction and returns the latest id")
    void pollTransactionsProcessesTransactions() throws Exception {
        setField(ledgerReader, "callback", callback);
        Transaction first = transactionWithId(7L);
        Transaction second = transactionWithId(9L);
        when(dbRepo.findLatest(anyLong()))
            .thenReturn(Arrays.asList(first, second));

        long result = invokePollTransactions(5L);

        assertEquals(9L, result);
        verify(callback).processTransaction(first);
        verify(callback).processTransaction(second);
    }

    @Test
    @DisplayName("getLatestTransactionId returns the starting id when the ledger is empty")
    void getLatestTransactionIdReturnsStartingIdWhenNull() throws Exception {
        when(dbRepo.latestTransactionId()).thenReturn(null);

        assertEquals(STARTING_TRANSACTION_ID, invokeGetLatestTransactionId());
    }

    @Test
    @DisplayName("getLatestTransactionId returns the id reported by the repository")
    void getLatestTransactionIdReturnsRepositoryValue() throws Exception {
        when(dbRepo.latestTransactionId()).thenReturn(99L);

        assertEquals(99L, invokeGetLatestTransactionId());
    }

    // --- helpers ---------------------------------------------------------

    private Transaction transactionWithId(long id) {
        Transaction transaction = mock(Transaction.class);
        when(transaction.getTransactionId()).thenReturn(id);
        return transaction;
    }

    private Thread getBackgroundThread() {
        return (Thread) getField(ledgerReader, "backgroundThread");
    }

    private void joinBackgroundThread() throws InterruptedException {
        Thread background = getBackgroundThread();
        if (background != null) {
            background.join(THREAD_JOIN_TIMEOUT_MS);
        }
    }

    private static void waitUntilSleeping(Thread thread)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + THREAD_JOIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (thread.getState() == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(10);
        }
    }

    private long invokePollTransactions(long startingId) throws Exception {
        Method method = LedgerReader.class
            .getDeclaredMethod("pollTransactions", long.class);
        method.setAccessible(true);
        return (long) method.invoke(ledgerReader, startingId);
    }

    private long invokeGetLatestTransactionId() throws Exception {
        Method method = LedgerReader.class
            .getDeclaredMethod("getLatestTransactionId");
        method.setAccessible(true);
        return (long) method.invoke(ledgerReader);
    }

    private static void setField(Object target, String name, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object getField(Object target, String name) {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long getLongField(Object target, String name) {
        return (long) getField(target, name);
    }
}
