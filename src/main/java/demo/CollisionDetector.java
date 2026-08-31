package demo;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Counts owners that ever shared a resource. It records every acquire instead of polling who
 *  holds what, because polling would miss a collision that opens and closes between two looks.
 *  It watches only what the workload did, so all three rows are scored the same way. */
final class CollisionDetector {

    private final Map<String, Set<String>> ownersByResource = new ConcurrentHashMap<>();
    private final Set<String> collided = ConcurrentHashMap.newKeySet();

    void recordAcquired(String resourceId, String owner) {
        Set<String> owners =
                ownersByResource.computeIfAbsent(resourceId, id -> ConcurrentHashMap.newKeySet());
        owners.add(owner);
        // Checked after the add, so this can only undercount, never invent a collision.
        if (owners.size() > 1) {
            collided.addAll(owners);
        }
    }

    void recordReleased(String resourceId, String owner) {
        // A bare NPE here would read as "only N of 24 tests completed" and blame the strategy.
        Objects.requireNonNull(ownersByResource.get(resourceId), resourceId + " was never acquired")
                .remove(owner);
    }

    /** Owners, not incidents: cannot exceed the number of owners in the run. */
    int collidedOwnerCount() {
        return collided.size();
    }

    /** Distinct resources the workload ever touched. A resource stays counted after its owners go. */
    int resourcesUsed() {
        return ownersByResource.size();
    }
}
