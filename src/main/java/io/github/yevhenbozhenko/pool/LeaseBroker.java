package io.github.yevhenbozhenko.pool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/** Safe and fast, via three ideas: claim-only-if-free as one indivisible step,
 *  {@link #acquire(String, Selector)} so callers ask for what they need instead of naming an id,
 *  and leases that expire so a dead owner cannot shrink the pool.
 *
 *  @param <T> the payload type the pool's resources carry */
public final class LeaseBroker<T> implements ResourcePool<T> {

    /** Bounds how long an expired lease can go unnoticed while somebody is waiting. */
    private static final long RECLAIM_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(20);

    /** One resource plus who holds it. The mutable fields change only under {@link #lock}, which is
     *  what makes "is it free?" and "it is mine now" a single step instead of two. Private, because a
     *  holder the outside world can reach is the bug this class exists to prevent. */
    private static final class Slot<T> {

        private final Resource<T> resource;
        private Lease<T> lease;
        private long expiresAt;

        Slot(Resource<T> resource) {
            this.resource = resource;
        }
    }

    private final List<Slot<T>> slots;

    /** Sorted, so an unsatisfiable-selector diagnostic lists the alternatives in a stable order. */
    private final Set<String> offeredTags;

    private final long leaseTtlNanos;
    private final long acquireTimeoutNanos;

    /** Fair, so newcomers cannot barge past threads already queued. That is a promise about the
     *  lock, not about the resource: a waiter whose poll expires goes to the back of the queue, so
     *  later callers are often served first. It buys progress, not turn-taking. */
    private final ReentrantLock lock = new ReentrantLock(true);

    private final Condition resourceReleased = lock.newCondition();

    /** A steady timer, not the wall clock: an NTP correction must not expire every lease at once. */
    private final LongSupplier nanoTime;

    /** Guarded by {@link #lock}. */
    private int reclaimedLeases;

    /** {@code leaseTtlMillis} must exceed the longest legitimate hold, or live work gets reclaimed
     *  underneath it. The resources must belong to this broker alone: a second pool over the same
     *  ids keeps its own books. */
    public LeaseBroker(List<Resource<T>> resources, long leaseTtlMillis, long acquireTimeoutMillis) {
        this(resources, leaseTtlMillis, acquireTimeoutMillis, System::nanoTime);
    }

    /** Lets a test supply its own clock so a lease can be expired instantly. It drives expiry only;
     *  the acquire deadline stays on the real clock, so a frozen clock cannot hang the suite. */
    LeaseBroker(List<Resource<T>> resources, long leaseTtlMillis, long acquireTimeoutMillis,
            LongSupplier nanoTime) {
        if (resources.isEmpty() || leaseTtlMillis <= 0 || acquireTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, a positive ttl and a non-negative timeout");
        }
        List<Slot<T>> built = new ArrayList<>();
        Set<String> tags = new TreeSet<>();
        Set<String> seenIds = new HashSet<>();
        for (Resource<T> resource : resources) {
            // Callers name a resource by its id and nothing else, so duplicates make it ambiguous.
            if (!seenIds.add(resource.id())) {
                throw new IllegalArgumentException(
                        "resource ids must be unique, saw " + resource.id() + " twice");
            }
            built.add(new Slot<>(resource));
            tags.addAll(resource.tags());
        }
        this.slots = List.copyOf(built);
        this.offeredTags = tags;
        this.leaseTtlNanos = TimeUnit.MILLISECONDS.toNanos(leaseTtlMillis);
        this.acquireTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        this.nanoTime = nanoTime;
    }

    /** Lease any free resource the selector accepts.
     *
     *  @throws IllegalArgumentException if no resource in the pool matches {@code selector} at all
     *  @throws NoResourceAvailableException if nothing frees up within the acquire timeout */
    @Override
    public Lease<T> acquire(String owner, Selector selector) throws InterruptedException {
        // Both fail badly if left to chance: a null owner waits out the whole timeout first, and a
        // null selector dies inside the lambda below as "<parameter1> is null".
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(selector, "selector");
        // Before any waiting: this is the difference between a typo and a timeout. Asked of the
        // pool, not of a tag list, so it works for a hand-written selector too.
        if (slots.stream().noneMatch(slot -> selector.matches(slot.resource.tags()))) {
            throw new IllegalArgumentException("no resource in this pool matches " + selector
                    + "; the pool offers " + offeredTags);
        }
        // Real clock, not the injected one: a frozen test clock must not turn a timeout into a hang.
        long start = System.nanoTime();
        long deadline = start + acquireTimeoutNanos;
        lock.lock();
        try {
            while (true) {
                // On the acquiring thread: a waiter is exactly who cares. No background thread.
                reclaimExpiredLeases();
                for (Slot<T> slot : slots) {
                    if (slot.lease == null && selector.matches(slot.resource.tags())) {
                        slot.lease = new Lease<>(this, slot.resource, owner);
                        slot.expiresAt = nanoTime.getAsLong() + leaseTtlNanos;
                        return slot.lease;
                    }
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new NoResourceAvailableException(owner, selector,
                            Duration.ofNanos(System.nanoTime() - start));
                }
                resourceReleased.await(Math.min(remaining, RECLAIM_POLL_NANOS), TimeUnit.NANOSECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Frees <em>this exact lease</em>, never the owner's name. An expired lease may already have
     *  been re-issued, even to the same owner, so matching on the name would rob the live holder.
     *  A stale lease sits in no slot, so this quietly does nothing. */
    @Override
    public void release(Lease<T> lease) {
        // Null would match the first free slot and report a release that never happened.
        Objects.requireNonNull(lease, "lease");
        lock.lock();
        try {
            for (Slot<T> slot : slots) {
                if (slot.lease == lease) {
                    slot.lease = null;
                    // Waiters want different tags, so only they can tell whether this helps.
                    resourceReleased.signalAll();
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public int reclaimedLeaseCount() {
        lock.lock();
        try {
            return reclaimedLeases;
        } finally {
            lock.unlock();
        }
    }

    /** Who holds {@code resourceId}, for diagnostics. Empty for a free resource and for an id this
     *  pool does not have. An expired lease still reads as held until the next acquire clears it. */
    public Optional<String> holderOf(String resourceId) {
        lock.lock();
        try {
            for (Slot<T> slot : slots) {
                if (slot.resource.id().equals(resourceId)) {
                    return Optional.ofNullable(slot.lease).map(Lease::owner);
                }
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /** Frees leases whose owner is gone: crashed process, killed CI job, forgotten teardown. */
    private void reclaimExpiredLeases() {
        long now = nanoTime.getAsLong();
        for (Slot<T> slot : slots) {
            if (slot.lease != null && slot.expiresAt - now <= 0) {
                slot.lease = null;
                reclaimedLeases++;
            }
        }
    }
}
