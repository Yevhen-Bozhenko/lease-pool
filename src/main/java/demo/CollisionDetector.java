package demo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Counts owners that ever shared an account by registration, not sampling: polling
 *  {@code isHeldBy} would miss a collision that opens and closes between polls. Observes only what
 *  the workload did, so all three rows are scored by identical rules. */
final class CollisionDetector {

    private final Map<String, Set<String>> ownersByAccount = new ConcurrentHashMap<>();
    private final Set<String> collided = ConcurrentHashMap.newKeySet();

    void recordAcquired(String accountId, String owner) {
        Set<String> owners =
                ownersByAccount.computeIfAbsent(accountId, id -> ConcurrentHashMap.newKeySet());
        owners.add(owner);
        // Checked after the add, so this can only undercount, never invent a collision.
        if (owners.size() > 1) {
            collided.addAll(owners);
        }
    }

    void recordReleased(String accountId, String owner) {
        Set<String> owners = ownersByAccount.get(accountId);
        if (owners != null) {
            owners.remove(owner);
        }
    }

    /** Owners, not incidents: cannot exceed the number of owners in the run. */
    int collidedOwnerCount() {
        return collided.size();
    }

    /** Distinct accounts the workload ever touched; keys outlive the owners registered against them. */
    int accountsUsed() {
        return ownersByAccount.size();
    }
}
