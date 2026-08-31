package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.ResourcePool;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** The one claim about strategy 2 the benchmark cannot make: a lease over some <em>other</em>
 *  resource must not free a live one here. The benchmark only ever releases its own leases.
 *
 *  <p>Deliberately not pinned, because it is false: a lease naming this owner <em>and</em> this
 *  resource id does free the live hold. Strategy 2 matches on the label, not on lease identity —
 *  see {@code StaticAssignment.release}. Tracking identity is what the broker is for. */
class StaticAssignmentTest {

    @Test
    @DisplayName("a lease over another resource does not free this owner's resource")
    // Separate thread, though not for the usual reason: a same-thread timeout does interrupt, and
    // that is enough for the Object.wait() this strategy parks in. It is not enough for the
    // synchronized blocks either side of that wait — monitor entry ignores interrupts, so a
    // lock-ordering regression would hang the build rather than fail it.
    @Timeout(value = 5, threadMode = SEPARATE_THREAD)
    void leaseOverAnotherResourceDoesNotRelease() throws Exception {
        List<Resource<String>> pool = List.of(new Resource<>("ACC-01", "login-for-ACC-01"));
        // Two owners at 2 per id, so both are pinned to ACC-01: if t1's hold breaks, t2 takes it.
        StaticAssignment<String> assignment = new StaticAssignment<>(pool, List.of("t1", "t2"), 2);
        Lease<String> live = assignment.acquire("t1");

        // A lease over a resource this strategy never issued, carrying an owner label it knows.
        // Lease's constructor is public because ResourcePool is, so no reflection is needed.
        ResourcePool<String> elsewhere = new NaiveSharedList<>(
                List.of(new Resource<>("OTHER-01", "login-for-OTHER-01")), 0);
        assignment.release(new Lease<>(elsewhere, new Resource<>("OTHER-01", "payload"), "t1"));

        // The only observable is whether t1's resource became available: the slots are private.
        AtomicBoolean stolen = new AtomicBoolean();
        AtomicReference<RuntimeException> failed = new AtomicReference<>();
        Thread contender = new Thread(() -> {
            try {
                assignment.acquire("t2");
                stolen.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();   // expected: t2 must still be waiting
            } catch (RuntimeException e) {
                // Uncaught, this would reach the default handler and leave stolen false, so an
                // acquire that threw would pass this test by never getting far enough to fail it.
                failed.set(e);
            }
        });
        contender.setDaemon(true);
        contender.start();
        contender.join(300);
        contender.interrupt();

        // Read once: the contender was interrupted, not joined, so it can still be writing.
        RuntimeException failure = failed.get();
        assertNull(failure, "t2's acquire threw, so this test proved nothing: " + failure);
        assertFalse(stolen.get(), "t2 acquired ACC-01 while t1 still held it");
        live.close();
    }
}
