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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.dao.DataAccessResourceFailureException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

class LedgerReaderTest {

    private LedgerReader ledgerReader;

    @Mock
    private TransactionRepository dbRepo;
    @Mock
    private LedgerReaderCallback callback;

    @BeforeEach
    void setUp() throws Exception {
        initMocks(this);
        ledgerReader = new LedgerReader();
        setField("dbRepo", dbRepo);
        // Keep polling fast so the background thread iterates quickly.
        setField("pollMs", 10);
        setField("localRoutingNum", "123456789");
    }

    private void setField(String name, Object value) throws Exception {
        Field field = LedgerReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(ledgerReader, value);
    }

    private long invokePollTransactions(long startingId) throws Exception {
        Method method = LedgerReader.class
            .getDeclaredMethod("pollTransactions", long.class);
        method.setAccessible(true);
        return (long) method.invoke(ledgerReader, startingId);
    }

    private Transaction transactionWithId(long id) {
        Transaction transaction = mock(Transaction.class);
        when(transaction.getTransactionId()).thenReturn(id);
        return transaction;
    }

    private void waitUntilNotAlive() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (ledgerReader.isAlive()
            && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("Given a null callback, startWithCallback throws")
    void startWithCallbackThrowsWhenCallbackIsNull() {
        assertThrows(IllegalStateException.class,
            () -> ledgerReader.startWithCallback(null));
    }

    @Test
    @DisplayName("A freshly constructed LedgerReader reports alive")
    void isAliveTrueBeforeThreadStarts() {
        assertTrue(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("New transactions are polled and passed to the callback")
    void startWithCallbackPollsNewTransactions() throws Exception {
        // init returns 0, loop sees remote 2 (>0) -> poll, then 1 (<2) -> stop
        when(dbRepo.latestTransactionId())
            .thenReturn(0L, 2L, 1L);
        Transaction transaction = transactionWithId(2L);
        AtomicReference<Transaction> processed = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        LedgerReaderCallback latchedCallback = t -> {
            processed.set(t);
            latch.countDown();
        };
        when(dbRepo.findLatest(0L))
            .thenReturn(Collections.singletonList(transaction));

        ledgerReader.startWithCallback(latchedCallback);

        assertTrue(latch.await(5, TimeUnit.SECONDS),
            "callback should be invoked for the new transaction");
        assertSame(transaction, processed.get());
        // Remote id going backwards should stop the background thread.
        waitUntilNotAlive();
        assertFalse(ledgerReader.isAlive());
        verify(dbRepo).findLatest(0L);
    }

    @Test
    @DisplayName("Database errors at init and while polling are tolerated")
    void startWithCallbackToleratesDatabaseErrors() throws Exception {
        // init throws, loop throws once, then remote id -2 (< -1) stops thread
        when(dbRepo.latestTransactionId())
            .thenThrow(new DataAccessResourceFailureException("down"))
            .thenThrow(new DataAccessResourceFailureException("down"))
            .thenReturn(-2L);

        ledgerReader.startWithCallback(callback);

        waitUntilNotAlive();
        assertFalse(ledgerReader.isAlive());
        verify(dbRepo, never()).findLatest(anyLong());
        verify(callback, never()).processTransaction(any());
    }

    @Test
    @DisplayName("pollTransactions forwards every transaction to the callback")
    void pollTransactionsProcessesAllTransactions() throws Exception {
        setField("callback", callback);
        Transaction first = transactionWithId(3L);
        Transaction second = transactionWithId(7L);
        when(dbRepo.findLatest(1L))
            .thenReturn(List.of(first, second));

        long latestId = invokePollTransactions(1L);

        assertEquals(7L, latestId);
        verify(callback).processTransaction(first);
        verify(callback).processTransaction(second);
    }

    @Test
    @DisplayName("pollTransactions returns the starting id when there is nothing new")
    void pollTransactionsReturnsStartingIdWhenEmpty() throws Exception {
        setField("callback", callback);
        when(dbRepo.findLatest(5L))
            .thenReturn(Collections.emptyList());

        long latestId = invokePollTransactions(5L);

        assertEquals(5L, latestId);
        verify(callback, never()).processTransaction(any());
    }
}
