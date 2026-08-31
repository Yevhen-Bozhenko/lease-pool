package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.ResourcePool;
import io.github.yevhenbozhenko.pool.Selector;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Strategy 1 — scan a shared list and take the first resource that looks free. Nothing locks, so
 *  two callers can both see the same one free and both take it. The fastest row, and wrong. */
public final class NaiveSharedList<T> implements ResourcePool<T> {

    /** A wedge guard, not a queueing policy: it bounds the spin, it does not turn it into a wait. */
    private static final long DEFAULT_SPIN_LIMIT_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private final List<Slot<T>> slots;
    private final long raceWindowMillis;
    private final long spinLimitNanos;

    public NaiveSharedList(List<Resource<T>> resources, long raceWindowMillis) {
        this(resources, raceWindowMillis, DEFAULT_SPIN_LIMIT_MILLIS);
    }

    /** Lets a test shorten the limit so it can be reached without a 30 s wait. Milliseconds. */
    NaiveSharedList(List<Resource<T>> resources, long raceWindowMillis, long spinLimitMillis) {
        // An empty pool would otherwise spin out the whole limit before naming the real mistake.
        if (resources.isEmpty() || raceWindowMillis < 0 || spinLimitMillis <= 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, a non-negative race window and a positive spin limit");
        }
        // Like the other two: a repeated entry gets a second Slot that release can never reach.
        if (resources.stream().map(Resource::id).distinct().count() != resources.size()) {
            throw new IllegalArgumentException("resource ids must be unique");
        }
        this.slots = resources.stream().map(Slot::new).toList();
        this.raceWindowMillis = raceWindowMillis;
        this.spinLimitNanos = TimeUnit.MILLISECONDS.toNanos(spinLimitMillis);
    }

    /** No waiting and no fairness. The one concession is the spin limit, so a fully-held pool fails
     *  loudly instead of pinning a core until you kill the JVM.
     *
     *  <p>It also cannot tell a typo from a busy pool: {@code LeaseBroker} rejects an impossible
     *  selector up front, while here the same typo spins out the limit and is reported as "busy".
     *
     *  @throws IllegalStateException if nothing looks free before the spin limit elapses */
    @Override
    public Lease<T> acquire(String owner, Selector selector) throws InterruptedException {
        long deadline = System.nanoTime() + spinLimitNanos;
        while (true) {
            for (Slot<T> slot : slots) {
                if (selector.matches(slot.resource.tags()) && !slot.isInUse()) {  // (1) CHECK
                    widenRaceWindow();
                    slot.forceHold(owner);      // (2) ACT — last writer wins, and every earlier
                    return new Lease<>(this, slot.resource, owner); //  "owner" is evicted mid-test
                }
            }
            // Only on the losing path, so the winning one is unchanged. Thread.yield() never throws
            // and never clears the flag, so the spin has to check it itself.
            if (Thread.interrupted()) {
                throw new InterruptedException(owner + " interrupted while spinning for a resource");
            }
            // Compared by subtraction so the check still holds when the timer counter wraps around.
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException(owner + " spun for "
                        + TimeUnit.NANOSECONDS.toMillis(spinLimitNanos)
                        + " ms without seeing a free resource");
            }
            // Step aside rather than sleep: a 1 ms sleep really takes ~15 ms on Windows, and the
            // table would read as if resources were scarce rather than as a timer quirk.
            Thread.yield();
        }
    }

    /** THE RACE WINDOW, between the check and the claim above: nothing stops another thread passing
     *  the same check on the same slot. Widening it does not *cause* the bug — the scan races at
     *  0 too — it only makes the damage land identically on every run. */
    private void widenRaceWindow() throws InterruptedException {
        Thread.sleep(raceWindowMillis);
    }

    /** Frees the resource without checking we still hold it — a race loser frees the winner's, and
     *  that is the lesson. Matched on the resource itself, so the scan cannot pick the wrong slot. */
    @Override
    public void release(Lease<T> lease) {
        for (Slot<T> slot : slots) {
            if (slot.resource == lease.resource()) {
                slot.forceRelease();
                return;
            }
        }
    }
}
