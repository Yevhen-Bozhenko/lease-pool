package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.ResourcePool;
import io.github.yevhenbozhenko.pool.Selector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Strategy 2 — the old fix: give every owner one fixed resource ("this suite always logs in as
 *  ACC-03"), each guarded by its own monitor. Correct, and slower than it needs to be: owners wait
 *  for their own resource while other resources sit idle. */
public final class StaticAssignment<T> implements ResourcePool<T> {

    private final Map<String, Slot<T>> assignment;

    public StaticAssignment(List<Resource<T>> pool, List<String> owners, int ownersPerId) {
        if (pool.isEmpty() || owners.isEmpty() || ownersPerId <= 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, at least one owner and a positive owners-per-id");
        }
        // Two entries with one id would give two owners the same resource.
        if (pool.stream().map(Resource::id).distinct().count() != pool.size()) {
            throw new IllegalArgumentException("resource ids must be unique");
        }
        // 24 owners at 6 per id touch only 4 resources, so half of an 8-resource pool is never used.
        int ids = Math.min(pool.size(), (owners.size() + ownersPerId - 1) / ownersPerId);
        List<Slot<T>> slots = pool.stream().map(Slot::new).toList();
        Map<String, Slot<T>> byOwner = new HashMap<>();
        for (int i = 0; i < owners.size(); i++) {
            // A repeated label would overwrite an earlier binding and strand its resource.
            if (byOwner.put(owners.get(i), slots.get(i % ids)) != null) {
                throw new IllegalArgumentException(
                        "owner labels must be distinct, saw " + owners.get(i) + " twice");
            }
        }
        this.assignment = Map.copyOf(byOwner);
    }

    /** The selector is ignored, which is the point of this row: an owner gets its one configured
     *  resource or it waits, however well another one would have suited it. */
    @Override
    public Lease<T> acquire(String owner, Selector selector) throws InterruptedException {
        // Refused, not ignored: ResourcePool promises the resource matches the selector.
        if (Objects.requireNonNull(selector, "selector") != Selector.any()) {
            throw new IllegalArgumentException(
                    "strategy 2 pins each owner to one resource, so it cannot honour a selector");
        }
        Slot<T> mine = assignment.get(Objects.requireNonNull(owner, "owner"));
        if (mine == null) {
            throw new IllegalStateException("no static resource configured for " + owner);
        }
        // Waits for *this* resource even when others are free. That is what hard-coded means.
        synchronized (mine) {
            while (mine.isInUse()) {
                mine.wait();
            }
            // Safe despite forceHold(): every owner pinned here waits on this same monitor first.
            mine.forceHold(owner);
        }
        return new Lease<>(this, mine.resource, owner);
    }

    /** Ignores a lease it never issued rather than throwing, since that usually arrives from
     *  {@code Lease.close()} and would fail a passing test. Matching is by owner name only. */
    @Override
    public void release(Lease<T> lease) {
        Slot<T> slot = assignment.get(lease.owner());
        // releaseIfHeldBy checks only the owner name, so this id check stops a lease from another
        // pool freeing whatever that owner holds here.
        if (slot == null || !slot.resource.id().equals(lease.resource().id())) {
            return;
        }
        synchronized (slot) {
            slot.releaseIfHeldBy(lease.owner());
            slot.notifyAll();
        }
    }
}
