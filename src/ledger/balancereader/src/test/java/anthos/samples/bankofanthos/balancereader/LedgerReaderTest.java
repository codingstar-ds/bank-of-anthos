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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.web.client.ResourceAccessException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

class LedgerReaderTest {

    private LedgerReader ledgerReader;

    @Mock
    private TransactionRepository dbRepo;

    private static final int POLL_MS = 20;
    private static final String LOCAL_ROUTING_NUM = "123456789";
    // Max time to wait for the background polling thread to terminate.
    private static final long THREAD_JOIN_TIMEOUT_MS = 5000;

    @BeforeEach
    void setUp() throws Exception {
        initMocks(this);
        ledgerReader = new LedgerReader();
        setField(ledgerReader, "dbRepo", dbRepo);
        setField(ledgerReader, "pollMs", POLL_MS);
        setField(ledgerReader, "localRoutingNum", LOCAL_ROUTING_NUM);
    }

    @Test
    @DisplayName("Given a null callback, startWithCallback throws IllegalStateException")
    void startWithCallbackThrowsWhenCallbackIsNull() {
        assertThrows(IllegalStateException.class,
            () -> ledgerReader.startWithCallback(null));
    }

    @Test
    @DisplayName("Before startWithCallback is called, isAlive returns true")
    void isAliveReturnsTrueBeforeStart() {
        assertTrue(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("startWithCallback loads the latest transaction id and starts the "
        + "background thread, which stops when the remote id is out of sync")
    void startWithCallbackStartsThreadThatStopsWhenRemoteIdOutOfSync()
            throws Exception {
        // init reads 10, then the background thread reads 5 (< 10) => out of sync
        when(dbRepo.latestTransactionId()).thenReturn(10L, 5L);

        LedgerReaderCallback callback = mock(LedgerReaderCallback.class);
        ledgerReader.startWithCallback(callback);
        joinBackgroundThread();

        // thread terminated because the remote id was lower than the local id
        assertFalse(ledgerReader.isAlive());
        verify(dbRepo, atLeastOnce()).latestTransactionId();
    }

    @Test
    @DisplayName("startWithCallback treats a null latest transaction id as the "
        + "starting id (-1)")
    void startWithCallbackHandlesNullLatestTransactionId() throws Exception {
        // init reads null (=> -1), then the thread reads -5 (< -1) => out of sync
        when(dbRepo.latestTransactionId()).thenReturn(null, -5L);

        ledgerReader.startWithCallback(mock(LedgerReaderCallback.class));
        joinBackgroundThread();

        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("When new transactions are available, the background thread polls "
        + "them and invokes the callback for each one")
    void backgroundThreadProcessesNewTransactions() throws Exception {
        // init reads 0, thread reads 3 (> 0) => poll, then reads 0 (< 3) => stop
        when(dbRepo.latestTransactionId()).thenReturn(0L, 3L, 0L);

        Transaction transaction = mock(Transaction.class);
        when(transaction.getTransactionId()).thenReturn(3L);
        List<Transaction> transactions = Collections.singletonList(transaction);
        when(dbRepo.findLatest(0L)).thenReturn(transactions);

        LedgerReaderCallback callback = mock(LedgerReaderCallback.class);
        ledgerReader.startWithCallback(callback);
        joinBackgroundThread();

        verify(dbRepo).findLatest(0L);
        verify(callback, times(1)).processTransaction(transaction);
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("A ResourceAccessException while loading the latest id at init is "
        + "handled gracefully")
    void startWithCallbackHandlesResourceAccessExceptionAtInit() throws Exception {
        when(dbRepo.latestTransactionId())
            .thenThrow(new ResourceAccessException("db down"))
            .thenReturn(-5L);

        ledgerReader.startWithCallback(mock(LedgerReaderCallback.class));
        joinBackgroundThread();

        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("A DataAccessResourceFailureException raised while polling is "
        + "handled and the thread keeps the last known id")
    void backgroundThreadHandlesDataAccessFailureWhilePolling() throws Exception {
        // init reads 7, thread throws once (keeps 7), then reads 0 (< 7) => stop
        when(dbRepo.latestTransactionId())
            .thenReturn(7L)
            .thenThrow(new DataAccessResourceFailureException("db down"))
            .thenReturn(0L);

        ledgerReader.startWithCallback(mock(LedgerReaderCallback.class));
        joinBackgroundThread();

        assertFalse(ledgerReader.isAlive());
        verify(dbRepo, atLeastOnce()).latestTransactionId();
    }

    @Test
    @DisplayName("When the remote id keeps matching the local id, the thread stays "
        + "alive and polling until interrupted")
    void backgroundThreadStaysAliveWhenRemoteIdMatches() throws Exception {
        // The stub reads from an atomic so the main thread can change the answer
        // without re-stubbing while the background thread is invoking the mock.
        // Starts at 4 (== local id => thread keeps polling and stays alive).
        AtomicLong remoteId = new AtomicLong(4L);
        when(dbRepo.latestTransactionId()).thenAnswer(invocation -> remoteId.get());

        ledgerReader.startWithCallback(mock(LedgerReaderCallback.class));
        // give the thread time to run a few poll cycles
        Thread.sleep(POLL_MS * 3L);
        assertTrue(ledgerReader.isAlive());

        // interrupting only aborts the current sleep; the loop keeps polling
        interruptBackgroundThread();
        assertTrue(ledgerReader.isAlive());

        // drop the remote id below the local id so the loop exits and the
        // test does not leak a running thread
        remoteId.set(0L);
        joinBackgroundThread();
        assertFalse(ledgerReader.isAlive());
    }

    private void joinBackgroundThread() throws Exception {
        Thread thread = (Thread) getField(ledgerReader, "backgroundThread");
        if (thread != null) {
            thread.join(THREAD_JOIN_TIMEOUT_MS);
        }
    }

    private void interruptBackgroundThread() throws Exception {
        Thread thread = (Thread) getField(ledgerReader, "backgroundThread");
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static void setField(Object target, String name, Object value)
            throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
