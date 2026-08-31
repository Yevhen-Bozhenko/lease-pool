package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.ResourcePool;
import io.github.yevhenbozhenko.pool.Selector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Strategy 2 — the legacy fix: bind every owner to one resource up front ("this suite always logs
 *  in as ACC-03"), with a separate monitor per resource. Correct, and slower than it needs to be:
 *  owners queue behind their assigned resource while other resources sit idle. */
public final class StaticAssignment<T> implements ResourcePool<T> {

    private final Map<String, Slot<T>> assignment;

    public StaticAssignment(List<Resource<T>> pool, List<String> owners, int ownersPerId) {
        if (pool.isEmpty() || owners.isEmpty() || ownersPerId <= 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, at least one owner and a positive owners-per-id");
        }
        // One Slot per entry, so a repeated resource would let two owners each "hold" it — a
        // collision in the row whose whole claim is that it has none.
        if (pool.stream().map(Resource::id).distinct().count() != pool.size()) {
            throw new IllegalArgumentException("resource ids must be unique");
        }
        // How many resources the config actually names: 24 owners at 6 per id touch 4, so half of
        // an 8-resource pool is dead weight however busy the suite gets.
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

    /** The selector is ignored, which is the whole point of the row: an owner gets its one
     *  configured resource or it waits, however well something else would have suited it. */
    @Override
    public Lease<T> acquire(String owner, Selector selector) throws InterruptedException {
        // Refused, not ignored: ResourcePool promises the resource matches, and this strategy
        // cannot honour a selector at all. Null is checked first, or Map.get names nothing.
        if (Objects.requireNonNull(selector, "selector") != Selector.any()) {
            throw new IllegalArgumentException(
                    "strategy 2 pins each owner to one resource, so it cannot honour a selector");
        }
        Slot<T> mine = assignment.get(Objects.requireNonNull(owner, "owner"));
        if (mine == null) {
            throw new IllegalStateException("no static resource configured for " + owner);
        }
        // Wait for *this* resource, however many others sit idle. No falling back to a free one:
        // that is what "hard-coded" means, and why this row is slow rather than wrong.
        synchronized (mine) {
            while (mine.isInUse()) {
                mine.wait();
            }
            // Safe despite forceHold(): every owner pinned here waits on this same monitor first.
            mine.forceHold(owner);
        }
        return new Lease<>(this, mine.resource, owner);
    }

    /** Ignores a lease it never issued instead of throwing. {@code ResourcePool} asks for that, and
     *  this usually arrives from {@code Lease.close()}, where throwing would fail a passing test.
     *  It matches on the owner name, not on the lease, so a hand-built lease naming this owner and
     *  this resource is indistinguishable — telling those apart is the broker's job. */
    @Override
    public void release(Lease<T> lease) {
        Slot<T> slot = assignment.get(lease.owner());
        // releaseIfHeldBy checks only the owner name, so this id check is what stops a lease from
        // another pool freeing whatever that owner holds here.
        if (slot == null || !slot.resource.id().equals(lease.resource().id())) {
            return;
        }
        synchronized (slot) {
            slot.releaseIfHeldBy(lease.owner());
            slot.notifyAll();
        }
    }
}
