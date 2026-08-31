package demo;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Counts owners that ever shared a resource by registration, not sampling: polling the holder
 *  would miss a collision that opens and closes between polls. Observes only what the workload did,
 *  so all three rows are scored by identical rules. */
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
        // A release always follows the acquire that created the key. Said out loud, because a bare
        // NPE here would surface as "only N of 24 tests completed" and blame the strategy.
        Objects.requireNonNull(ownersByResource.get(resourceId), resourceId + " was never acquired")
                .remove(owner);
    }

    /** Owners, not incidents: cannot exceed the number of owners in the run. */
    int collidedOwnerCount() {
        return collided.size();
    }

    /** Distinct resources the workload ever touched; keys outlive the owners registered on them. */
    int resourcesUsed() {
        return ownersByResource.size();
    }
}
