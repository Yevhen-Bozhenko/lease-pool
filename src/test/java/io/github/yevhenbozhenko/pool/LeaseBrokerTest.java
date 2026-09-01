package io.github.yevhenbozhenko.pool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Pins the correctness claims the README makes about {@link LeaseBroker} — the benchmark cannot
 *  catch these: its 5 s TTL against 40 ms of work hides a stale release entirely. */
class LeaseBrokerTest {

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
     *  over a 4-resource pool, short enough to stay far under the 5 s lease TTL. */
    private static void holdBriefly() throws InterruptedException {
        Thread.sleep(5);
    }

    private static List<Resource<String>> newPool(int size) {
        List<Resource<String>> resources = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            String id = String.format("ACC-%02d", i);
            resources.add(new Resource<>(id, "login-for-" + id));
        }
        return resources;
    }

    @Test
    @DisplayName("never issues one resource to two owners at the same time")
    // Separate thread, because a deadlock regression would hang inside ExecutorService.close(),
    // which waits for termination and would otherwise stall the whole build with no diagnostic.
    @Timeout(value = 30, threadMode = SEPARATE_THREAD)
    void neverDoubleIssues() throws Exception {
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(4), 5_000, 10_000);
        // Counted rather than sampled: a poll would miss an overlap that opens and closes between
        // two reads, and an overlap missed is the one thing this test exists to catch.
        Map<String, AtomicInteger> insideNow = new ConcurrentHashMap<>();
        Set<String> collided = ConcurrentHashMap.newKeySet();
        Set<String> completed = ConcurrentHashMap.newKeySet();
        Set<String> failures = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(24)) {
            for (int i = 0; i < 24; i++) {
                String owner = String.format("test-%02d", i);
                executor.execute(() -> {
                    try {
                        start.await();
                        try (Lease<String> lease = broker.acquire(owner)) {
                            AtomicInteger inside = insideNow.computeIfAbsent(
                                    lease.resource().id(), id -> new AtomicInteger());
                            if (inside.incrementAndGet() > 1) {
                                collided.add(owner);
                            }
                            holdBriefly();
                            inside.decrementAndGet();
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
        assertEquals(24, completed.size(), "every owner should have actually leased a resource");
        assertEquals(Set.of(), collided, "24 owners shared a 4-resource pool without overlap");
    }

    @Test
    @DisplayName("reclaims an expired lease from an owner that never releases")
    void reclaimsExpiredLease() throws Exception {
        FakeClock clock = new FakeClock();
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(1), 50, 5_000, clock);

        broker.acquire("crashed-test"); // deliberately never closed
        clock.advance(Duration.ofMillis(51));

        assertEquals("ACC-01", broker.acquire("next-test").resource().id());
        assertEquals(1, broker.reclaimedLeaseCount());
        assertEquals(Optional.of("next-test"), broker.holderOf("ACC-01"));
    }

    @Test
    @DisplayName("a waiter already blocked when a lease expires is woken by the reclaim poll")
    void parkedWaiterIsWokenByTheReclaimPoll() throws Exception {
        // The real clock, deliberately: what is under test is the timed await() expiring, which a
        // frozen clock cannot drive. reclaimExpiredLeases() never signals the condition, so the poll
        // is the only thing that lets an already-parked waiter notice an expired lease.
        // TTL far under the acquire timeout, so only a reclaim — never the timeout — can end the wait.
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(1), 300, 5_000);
        broker.acquire("crashed-test"); // deliberately never closed

        AtomicReference<String> got = new AtomicReference<>();
        AtomicLong waitedMillis = new AtomicLong();
        // Timed from this thread: started inside the runnable it would begin after Thread.start()
        // latency, which comes straight out of the lower bound's margin against the 300 ms ttl.
        long startNanos = System.nanoTime();
        Thread waiter = new Thread(() -> {
            try {
                got.set(broker.acquire("next-test").resource().id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                waitedMillis.set(Duration.ofNanos(System.nanoTime() - startNanos).toMillis());
            }
        });
        waiter.setDaemon(true);
        waiter.start();
        waiter.join(8_000);

        assertEquals("ACC-01", got.get(), "the waiter should be handed the reclaimed resource");
        assertEquals(1, broker.reclaimedLeaseCount());
        // The lower bound proves the waiter genuinely parked rather than arriving after expiry; the
        // upper bound is what fails if the poll goes away, since it then sleeps out the whole 5 s.
        assertTrue(waitedMillis.get() > 250 && waitedMillis.get() < 2_000,
                "expected a wait of roughly the 300 ms ttl, got: " + waitedMillis.get() + " ms");
    }

    @Test
    @DisplayName("a stale release does not strip the resource from its new holder")
    void staleReleaseIsANoOp() throws Exception {
        FakeClock clock = new FakeClock();
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(1), 50, 5_000, clock);

        Lease<String> stale = broker.acquire("test-A");
        clock.advance(Duration.ofMillis(51)); // A's lease expires while A is still "running"
        Lease<String> fresh = broker.acquire("test-B"); // reclaims, then legitimately claims

        stale.close(); // A finally finishes and hands back a lease it lost long ago
        assertEquals(Optional.of("test-B"), broker.holderOf("ACC-01"),
                "stale release stripped the live holder — see the javadoc on LeaseBroker.release");

        fresh.close(); // ...and a legitimate release must still work
        assertEquals(Optional.empty(), broker.holderOf("ACC-01"));
    }

    @Test
    @DisplayName("a stale release is a no-op even when the new holder has the same owner label")
    void staleReleaseIsANoOpForTheSameOwner() throws Exception {
        FakeClock clock = new FakeClock();
        // Short acquire timeout: the assertThrows below deliberately waits it out.
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(1), 50, 100, clock);

        Lease<String> stale = broker.acquire("test-A");
        clock.advance(Duration.ofMillis(51));
        Lease<String> retry = broker.acquire("test-A"); // same label: a hung test that retried

        stale.close(); // the original finally unwinds
        assertEquals(Optional.of("test-A"), broker.holderOf("ACC-01"),
                "the retry's lease was freed by its own stale lease");
        // Owner, not just type: only the name says test-B was the one refused.
        NoResourceAvailableException thirdOwnerTimeout =
                assertThrows(NoResourceAvailableException.class, () -> broker.acquire("test-B"),
                        "the resource must still be held, so a third owner cannot take it");
        assertEquals("test-B", thirdOwnerTimeout.owner());

        retry.close();
        assertEquals(Optional.empty(), broker.holderOf("ACC-01"));
    }

    @Test
    @DisplayName("a selector skips resources that cannot serve it, and extra tags never disqualify")
    void acquireFiltersByTag() throws Exception {
        Resource<String> standard = new Resource<>("ACC-STD", Set.of("standard"), "std@test");
        // Two tags: an admin resource is also a perfectly good standard one.
        Resource<String> admin = new Resource<>("ACC-ADM", Set.of("standard", "admin"), "adm@test");
        LeaseBroker<String> broker = new LeaseBroker<>(List.of(standard, admin), 5_000, 5_000);

        try (Lease<String> lease = broker.acquire("test-A", Selector.tagged("admin"))) {
            assertEquals("ACC-ADM", lease.resource().id());
            assertEquals("adm@test", lease.get(), "the lease hands back the caller's own payload");
            assertEquals(Optional.empty(), broker.holderOf("ACC-STD"),
                    "the free standard-only resource must not be handed out for an admin request");
        }
        try (Lease<String> first = broker.acquire("test-B", Selector.tagged("standard"))) {
            assertEquals("ACC-STD", first.resource().id());
            try (Lease<String> spill = broker.acquire("test-C", Selector.tagged("standard"))) {
                assertEquals("ACC-ADM", spill.resource().id(),
                        "an admin resource carries 'standard' too, so it must still match");
            }
        }
    }

    @Test
    @DisplayName("a saturated pool reports itself as retryable, with the wait it actually cost")
    void acquireTimesOut() throws Exception {
        LeaseBroker<String> broker = new LeaseBroker<>(newPool(1), 5_000, 100);

        broker.acquire("test-A"); // holds the only resource for longer than the acquire timeout
        NoResourceAvailableException timeout =
                assertThrows(NoResourceAvailableException.class, () -> broker.acquire("test-B"));

        // Fields, not a parsed message: a caller deciding whether to back off reads these.
        assertEquals("test-B", timeout.owner());
        assertEquals(Selector.any(), timeout.selector(), "test-B asked for anything, not a tag");
        assertTrue(timeout.waited().toMillis() >= 100,
                "waited must cover the whole acquire timeout, got: " + timeout.waited());
    }

    @Test
    @DisplayName("a selector nothing in the pool matches fails immediately instead of waiting")
    void unsatisfiableSelectorFailsFast() {
        // Every resource is free, so only the up-front check can end this call quickly.
        LeaseBroker<String> broker = new LeaseBroker<>(
                List.of(new Resource<>("ACC-STD", Set.of("standard"), "std@test"),
                        new Resource<>("ACC-ADM", Set.of("admin"), "adm@test")),
                5_000, 2_000);

        long startNanos = System.nanoTime();
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> broker.acquire("test-A", Selector.tagged("standrad"))); // the typo is the point
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertTrue(elapsed.toMillis() < 500,
                "the check must not wait for the acquire timeout, took: " + elapsed);
        assertTrue(unknown.getMessage().contains("standrad"),
                "the diagnostic must name what was asked for: " + unknown.getMessage());
        assertTrue(unknown.getMessage().contains("standard") && unknown.getMessage().contains("admin"),
                "the diagnostic must list what the pool does offer: " + unknown.getMessage());
    }

    @Test
    @DisplayName("a tag combination no single resource carries also fails immediately")
    void selectorSpanningTwoResourcesFailsFast() {
        // One resource has 'standard', another has 'admin', and neither has both. The request is
        // unsatisfiable even though every tag in it exists somewhere in the pool.
        LeaseBroker<String> broker = new LeaseBroker<>(
                List.of(new Resource<>("ACC-STD", Set.of("standard"), "std@test"),
                        new Resource<>("ACC-ADM", Set.of("admin"), "adm@test")),
                5_000, 2_000);

        IllegalArgumentException spanning = assertThrows(IllegalArgumentException.class,
                () -> broker.acquire("test-A", Selector.tagged("standard", "admin")));

        // Both tags exist in the pool, so the offers half of the message names them either way.
        // Only the rendered selector shows the message repeating back what was asked for.
        assertTrue(spanning.getMessage().contains("tags [admin, standard]"),
                "the diagnostic must name the combination asked for: " + spanning.getMessage());
    }
}
