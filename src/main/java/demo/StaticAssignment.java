package demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Strategy 2 — the legacy fix: bind every owner to one account id up front ("this suite always
 *  logs in as ACC-03"), with a separate lock per account. Correct, and slower than it needs to be:
 *  owners queue behind their assigned account while other accounts sit idle. */
public final class StaticAssignment implements AccountStrategy {

    private final Map<String, Account> assignment;
    private final Map<String, Object> locks;

    public StaticAssignment(List<Account> pool, List<String> owners, int ownersPerId) {
        if (pool.isEmpty() || ownersPerId <= 0) {
            throw new IllegalArgumentException("need a non-empty pool and a positive owners-per-id");
        }
        // Distinct ids the legacy config actually mentions: 24 owners at 6 per id touch 4 accounts,
        // so half of an 8-account pool is dead weight however busy the suite gets.
        int ids = Math.min(pool.size(), Math.max(1, (owners.size() + ownersPerId - 1) / ownersPerId));
        Map<String, Account> byOwner = new HashMap<>();
        Map<String, Object> byId = new HashMap<>();
        for (int i = 0; i < owners.size(); i++) {
            byOwner.put(owners.get(i), pool.get(i % ids));
        }
        for (Account account : pool) {
            byId.put(account.id(), new Object());
        }
        this.assignment = Map.copyOf(byOwner);
        this.locks = Map.copyOf(byId);
    }

    @Override
    public Lease acquire(String owner) throws InterruptedException {
        Account mine = assignment.get(owner);
        if (mine == null) {
            throw new IllegalStateException("no static account configured for " + owner);
        }
        // Wait for *this* account specifically, however many others are idle. No fallback to a free
        // account: that is what "hard-coded" means, and why this strategy is slow rather than wrong.
        Object monitor = monitorFor(mine);
        synchronized (monitor) {
            while (mine.isInUse()) {
                monitor.wait();
            }
            // Safe despite forceHold(): every owner pinned to this id waits on this same lock first.
            mine.forceHold(owner);
        }
        return new Lease(this, mine, owner);
    }

    @Override
    public void release(Lease lease) {
        Object monitor = monitorFor(lease.account());
        synchronized (monitor) {
            lease.account().releaseIfHeldBy(lease.owner());
            monitor.notifyAll();
        }
    }

    private Object monitorFor(Account account) {
        Object monitor = locks.get(account.id());
        if (monitor == null) {
            throw new IllegalStateException(account.id() + " is not part of this strategy's pool");
        }
        return monitor;
    }
}
