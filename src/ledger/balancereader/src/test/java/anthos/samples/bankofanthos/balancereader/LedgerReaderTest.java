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
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.web.client.ResourceAccessException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

class LedgerReaderTest {

    private LedgerReader ledgerReader;

    @Mock
    private TransactionRepository dbRepo;
    @Mock
    private LedgerReaderCallback callback;

    private static final int POLL_MS = 20;
    private static final long TIMEOUT_MS = 3000L;

    @BeforeEach
    void setUp() throws Exception {
        initMocks(this);
        ledgerReader = new LedgerReader();
        setField(ledgerReader, "dbRepo", dbRepo);
        setField(ledgerReader, "pollMs", POLL_MS);
        setField(ledgerReader, "localRoutingNum", "123456789");
    }

    @Test
    @DisplayName("Given a null callback, startWithCallback throws IllegalStateException")
    void startWithCallbackThrowsWhenCallbackIsNull() {
        assertThrows(IllegalStateException.class,
            () -> ledgerReader.startWithCallback(null));
    }

    @Test
    @DisplayName("Before the background thread starts, isAlive returns true")
    void isAliveReturnsTrueWhenThreadNotStarted() {
        assertTrue(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("New transactions in the ledger are forwarded to the callback")
    void processesNewTransactions() {
        Transaction first = mockTransaction(1L);
        Transaction second = mockTransaction(2L);
        // init -> 0, first poll -> 2 (new), then out of sync (-1) to stop thread
        when(dbRepo.latestTransactionId()).thenReturn(0L, 2L).thenReturn(-1L);
        when(dbRepo.findLatest(0L)).thenReturn(Arrays.asList(first, second));

        ledgerReader.startWithCallback(callback);

        verify(callback, timeout(TIMEOUT_MS)).processTransaction(first);
        verify(callback, timeout(TIMEOUT_MS)).processTransaction(second);
    }

    @Test
    @DisplayName("When the remote ledger id goes backwards, the reader stops (out of sync)")
    void stopsWhenLedgerOutOfSync() {
        // init -> 5, then remote reports a lower id (2) -> out of sync -> thread dies
        when(dbRepo.latestTransactionId()).thenReturn(5L).thenReturn(2L);

        ledgerReader.startWithCallback(callback);

        waitForNotAlive();
        assertFalse(ledgerReader.isAlive());
        verify(callback, never()).processTransaction(org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("A database error during init is handled and the reader keeps running")
    void handlesDatabaseErrorDuringInit() {
        // init throws, then remote id (-2) is below the starting id (-1) -> thread stops
        when(dbRepo.latestTransactionId())
            .thenThrow(new ResourceAccessException("db down"))
            .thenReturn(-2L);

        ledgerReader.startWithCallback(callback);

        waitForNotAlive();
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("A database error while polling is handled and the reader keeps running")
    void handlesDatabaseErrorDuringPolling() {
        // init -> 0, poll throws (handled), then out of sync (-1) -> thread stops
        when(dbRepo.latestTransactionId())
            .thenReturn(0L)
            .thenThrow(new DataAccessResourceFailureException("db blip"))
            .thenReturn(-1L);

        ledgerReader.startWithCallback(callback);

        waitForNotAlive();
        assertFalse(ledgerReader.isAlive());
        verify(callback, never()).processTransaction(org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("A null latest transaction id is treated as the starting id")
    void treatsNullLatestIdAsStartingId() {
        // init -> null (=> starting id -1), then remote id (-2) < -1 -> thread stops
        when(dbRepo.latestTransactionId()).thenReturn(null).thenReturn(-2L);

        ledgerReader.startWithCallback(callback);

        waitForNotAlive();
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("Polling an empty result set does not invoke the callback")
    void emptyPollDoesNotInvokeCallback() {
        // init -> 0, poll finds new id 3 but returns no rows, then out of sync (-1)
        when(dbRepo.latestTransactionId()).thenReturn(0L, 3L).thenReturn(-1L);
        when(dbRepo.findLatest(0L)).thenReturn(Collections.emptyList());

        ledgerReader.startWithCallback(callback);

        waitForNotAlive();
        assertFalse(ledgerReader.isAlive());
        verify(callback, never()).processTransaction(org.mockito.ArgumentMatchers.any());
    }

    private void waitForNotAlive() {
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (ledgerReader.isAlive() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static Transaction mockTransaction(long id) {
        Transaction transaction = mock(Transaction.class);
        when(transaction.getTransactionId()).thenReturn(id);
        return transaction;
    }

    private static void setField(Object target, String name, Object value)
        throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
