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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

class LedgerReaderTest {

    private LedgerReader ledgerReader;

    @Mock
    private TransactionRepository dbRepo;

    private static final long STARTING_TRANSACTION_ID = -1;
    private static final String LOCAL_ROUTING_NUM = "123456789";
    private static final long AWAIT_TIMEOUT_MS = 3000;

    @BeforeEach
    void setUp() throws Exception {
        initMocks(this);
        ledgerReader = new LedgerReader();
        setField(ledgerReader, "dbRepo", dbRepo);
        setField(ledgerReader, "pollMs", 20);
        setField(ledgerReader, "localRoutingNum", LOCAL_ROUTING_NUM);
    }

    @Test
    @DisplayName("Given a null callback, startWithCallback throws IllegalStateException")
    void startWithCallbackThrowsWhenCallbackIsNull() {
        assertThrows(IllegalStateException.class,
            () -> ledgerReader.startWithCallback(null));
    }

    @Test
    @DisplayName("Before the background thread starts, isAlive returns true")
    void isAliveReturnsTrueBeforeStart() {
        assertTrue(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("Given new transactions in the ledger, the callback processes each one")
    void startWithCallbackProcessesNewTransactions() throws Exception {
        // Given: init reads id 5, then the poll loop sees a higher id (10),
        // then a lower id (0) which stops the background thread cleanly.
        when(dbRepo.latestTransactionId()).thenReturn(5L, 10L, 0L);
        Transaction transaction = mock(Transaction.class);
        when(transaction.getTransactionId()).thenReturn(10L);
        when(dbRepo.findLatest(5L)).thenReturn(List.of(transaction));

        final List<Transaction> processed =
            Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch latch = new CountDownLatch(1);

        // When
        ledgerReader.startWithCallback(t -> {
            processed.add(t);
            latch.countDown();
        });

        // Then
        assertTrue(latch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
            "callback was not invoked in time");
        verify(dbRepo).findLatest(5L);
        assertEquals(1, processed.size());
        assertEquals(transaction, processed.get(0));
        awaitUntil(() -> !ledgerReader.isAlive());
        assertFalse(ledgerReader.isAlive());
    }

    @Test
    @DisplayName("Given the ledger database is unreachable, the reader survives and stops on out-of-sync id")
    void startWithCallbackHandlesDatabaseErrors() throws Exception {
        // init failure, then a loop failure, then an out-of-sync id (< init id).
        when(dbRepo.latestTransactionId())
            .thenThrow(new DataAccessResourceFailureException("init down"))
            .thenThrow(new ResourceAccessException("loop down"))
            .thenReturn(-2L);

        ledgerReader.startWithCallback(t -> { });

        awaitUntil(() -> !ledgerReader.isAlive());
        assertFalse(ledgerReader.isAlive());
        verify(dbRepo, never()).findLatest(anyLong());
    }

    @Test
    @DisplayName("Given no transactions exist, getLatestTransactionId returns the starting id")
    void getLatestTransactionIdReturnsStartingIdWhenEmpty() throws Exception {
        when(dbRepo.latestTransactionId()).thenReturn(null);
        assertEquals(STARTING_TRANSACTION_ID, invokeGetLatestTransactionId());
    }

    @Test
    @DisplayName("Given transactions exist, getLatestTransactionId returns the latest id")
    void getLatestTransactionIdReturnsLatestId() throws Exception {
        when(dbRepo.latestTransactionId()).thenReturn(42L);
        assertEquals(42L, invokeGetLatestTransactionId());
    }

    private long invokeGetLatestTransactionId() throws Exception {
        Method method =
            LedgerReader.class.getDeclaredMethod("getLatestTransactionId");
        method.setAccessible(true);
        return (Long) method.invoke(ledgerReader);
    }

    private static void setField(Object target, String name, Object value)
        throws Exception {
        Field field = LedgerReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void awaitUntil(BooleanSupplier condition)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("condition not met within timeout");
    }
}
