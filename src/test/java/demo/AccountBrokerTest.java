package demo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pins the correctness claims the README makes about {@link AccountBroker} — the benchmark cannot
 *  catch these: its 5 s TTL against 40 ms of work hides a stale release entirely. */
class AccountBrokerTest {

    private static final String STANDARD = "STANDARD";

    /** Lets a TTL elapse instantly instead of sleeping through it, so the two expiry tests are
     *  deterministic rather than merely usually-right. */
    private static final class FakeClock implements LongSupplier {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long getAsLong() {
            return nanos.get();
        }

        void advance(Duration amount) {
            nanos.addAndGet(amount.toNanos());
        }
    }

    /** Stands in for whatever the owner came for — long enough that 24 of them genuinely contend
     *  over a 4-account pool, short enough to stay far under the 5 s lease TTL. */
    private static void holdBriefly() throws InterruptedException {
        Thread.sleep(5);
    }

    private static List<Account> newPool(int size) {
        List<Account> accounts = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            accounts.add(new Account(String.format("ACC-%02d", i), STANDARD));
        }
        return accounts;
    }

    @Test
    @DisplayName("never issues one account to two owners at the same time")
    void neverDoubleIssues() throws Exception {
        AccountBroker broker = new AccountBroker(newPool(4), 5_000, 10_000);
        CollisionDetector detector = new CollisionDetector();
        Set<String> completed = ConcurrentHashMap.newKeySet();
        Set<String> failures = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(24)) {
            for (int i = 0; i < 24; i++) {
                String owner = String.format("test-%02d", i);
                executor.execute(() -> {
                    try {
                        start.await();
                        try (Lease lease = broker.acquire(owner)) {
                            detector.recordAcquired(lease.account().id(), owner);
                            holdBriefly();
                            detector.recordReleased(lease.account().id(), owner);
                        }
                        completed.add(owner);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures.add(owner + ": interrupted");
                    } catch (RuntimeException e) {
                        failures.add(owner + ": " + e);
                    }
                });
            }
            start.countDown();
        } // close() waits for every task

        // executor.execute() sends an exception to the default handler, not to this thread. Without
        // these two assertions a run where every acquire threw still reports zero collisions.
        assertEquals(Set.of(), failures, "every owner should acquire and release cleanly");
        assertEquals(24, completed.size(), "every owner should have actually leased an account");
        assertEquals(0, detector.collidedOwnerCount(),
                "24 owners shared a 4-account pool without overlap");
    }

    @Test
    @DisplayName("reclaims an expired lease from an owner that never releases")
    void reclaimsExpiredLease() throws Exception {
        Account account = new Account("ACC-01", STANDARD);
        FakeClock clock = new FakeClock();
        AccountBroker broker = new AccountBroker(List.of(account), 50, 5_000, clock);

        broker.acquire("crashed-test"); // deliberately never closed
        clock.advance(Duration.ofMillis(51));

        assertEquals("ACC-01", broker.acquire("next-test").account().id());
        assertEquals(1, broker.reclaimedLeaseCount());
        assertTrue(account.isHeldBy("next-test"));
    }

    @Test
    @DisplayName("a stale release does not strip the account from its new holder")
    void staleReleaseIsANoOp() throws Exception {
        Account account = new Account("ACC-01", STANDARD);
        FakeClock clock = new FakeClock();
        AccountBroker broker = new AccountBroker(List.of(account), 50, 5_000, clock);

        Lease stale = broker.acquire("test-A");
        clock.advance(Duration.ofMillis(51)); // A's lease expires while A is still "running"
        Lease fresh = broker.acquire("test-B"); // reclaims, then legitimately claims

        stale.close(); // A finally finishes and hands back a lease it lost long ago
        assertTrue(account.isHeldBy("test-B"),
                "stale release stripped the live holder — see the javadoc on AccountBroker.release");

        fresh.close(); // ...and a legitimate release must still work
        assertFalse(account.isInUse());
    }

    @Test
    @DisplayName("a stale release is a no-op even when the new holder has the same owner label")
    void staleReleaseIsANoOpForTheSameOwner() throws Exception {
        Account account = new Account("ACC-01", STANDARD);
        FakeClock clock = new FakeClock();
        // Short acquire timeout: the assertThrows below deliberately waits it out.
        AccountBroker broker = new AccountBroker(List.of(account), 50, 100, clock);

        Lease stale = broker.acquire("test-A");
        clock.advance(Duration.ofMillis(51));
        Lease retry = broker.acquire("test-A"); // same label: a hung test that retried

        stale.close(); // the original finally unwinds
        assertTrue(account.isHeldBy("test-A"), "the retry's lease was freed by its own stale lease");
        // Owner, not just type: only the name says test-B was the one refused.
        NoAccountAvailableException thirdOwnerTimeout =
                assertThrows(NoAccountAvailableException.class, () -> broker.acquire("test-B"),
                        "the account must still be held, so a third owner cannot take it");
        assertEquals("test-B", thirdOwnerTimeout.owner());

        retry.close();
        assertFalse(account.isInUse());
    }

    @Test
    @DisplayName("acquire(capability) skips accounts that cannot serve the requested tag")
    void acquireFiltersByCapability() throws Exception {
        Account standard = new Account("ACC-STD", STANDARD);
        Account admin = new Account("ACC-ADM", "ADMIN");
        AccountBroker broker = new AccountBroker(List.of(standard, admin), 5_000, 5_000);

        try (Lease lease = broker.acquire("test-A", "ADMIN")) {
            assertEquals("ACC-ADM", lease.account().id());
            assertFalse(standard.isInUse(), "the free STANDARD account must not be handed out");
        }
        try (Lease lease = broker.acquire("test-B")) {
            assertEquals("ACC-STD", lease.account().id(), "no capability asked for: anything will do");
        }
    }

    @Test
    @DisplayName("a saturated pool reports itself as retryable, with the wait it actually cost")
    void acquireTimesOut() throws Exception {
        AccountBroker broker = new AccountBroker(newPool(1), 5_000, 100);

        broker.acquire("test-A"); // holds the only account for longer than the acquire timeout
        NoAccountAvailableException timeout =
                assertThrows(NoAccountAvailableException.class, () -> broker.acquire("test-B"));

        // Fields, not a parsed message: a caller deciding whether to back off reads these.
        assertEquals("test-B", timeout.owner());
        assertNull(timeout.capability(), "test-B asked for any account, not a tagged one");
        assertTrue(timeout.waited().toMillis() >= 100,
                "waited must cover the whole acquire timeout, got: " + timeout.waited());
    }

    @Test
    @DisplayName("a capability no account carries fails immediately instead of waiting it out")
    void unknownCapabilityFailsFast() {
        // Every account is free, so only the up-front check can end this call quickly.
        AccountBroker broker = new AccountBroker(
                List.of(new Account("ACC-STD", STANDARD), new Account("ACC-ADM", "ADMIN")),
                5_000, 2_000);

        long startNanos = System.nanoTime();
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> broker.acquire("test-A", "STANDRAD")); // the typo is the point
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertTrue(elapsed.toMillis() < 500,
                "the check must not wait for the acquire timeout, took: " + elapsed);
        assertTrue(unknown.getMessage().contains("STANDRAD"),
                "the diagnostic must name what was asked for: " + unknown.getMessage());
        assertTrue(unknown.getMessage().contains(STANDARD) && unknown.getMessage().contains("ADMIN"),
                "the diagnostic must list what the pool does offer: " + unknown.getMessage());
    }

    @Test
    @DisplayName("the naive strategy aborts rather than spinning forever on a fully-held pool")
    void naiveSpinIsBounded() throws Exception {
        List<Account> pool = newPool(1);
        pool.get(0).forceHold("someone-else"); // wedged: no scan will ever see a free account

        // 100 ms through the test seam, not the shipped 30 s: the guard is under test, not the wait.
        NaiveSharedList naive = new NaiveSharedList(pool, 0, 100);
        IllegalStateException wedged =
                assertThrows(IllegalStateException.class, () -> naive.acquire("test-1"));
        assertTrue(wedged.getMessage().contains("test-1"),
                "the diagnostic must name who gave up: " + wedged.getMessage());
    }
}
