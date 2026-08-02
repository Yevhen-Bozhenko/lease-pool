package demo;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/** Strategy 3 — safe and fast, via three ideas: claim-only-if-free in one step
 *  ({@code Account.tryHold}), {@link #acquire(String, String)} so callers ask for what they need
 *  instead of naming an id, and leases that expire so a dead owner cannot shrink the pool. */
public final class AccountBroker implements AccountStrategy {

    /** Bounds how long an expired lease can go unnoticed while somebody is waiting. */
    private static final long RECLAIM_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(20);

    private final List<Account> accounts;
    private final long leaseTtlNanos;
    private final long acquireTimeoutNanos;

    /** Sorted, so an unknown-capability diagnostic lists the alternatives in a stable order. */
    private final Set<String> capabilities;

    /** Fair, so callers are served in arrival order: a steady stream of newcomers cannot leave an
     *  earlier waiter queueing forever. Costs a little throughput, worth it for a small pool. */
    private final ReentrantLock lock = new ReentrantLock(true);

    private final Condition accountReleased = lock.newCondition();

    /** Expiry is measured with a steadily-ticking timer rather than the wall clock, so a clock
     *  correction cannot expire every live lease at once. Storing the {@link Lease} itself — not
     *  just the owner's name — lets {@link #release} tell two leases of one owner apart. */
    private record Held(Lease lease, long expiresAt) {
    }

    private final Map<String, Held> held = new HashMap<>();

    /** Guarded by {@link #lock}. */
    private int reclaimedLeases;

    /** Every reading of "now", so tests can expire a TTL without sleeping through it. */
    private final LongSupplier nanoTime;

    /** {@code leaseTtlMillis} must exceed the longest legitimate hold, or live work gets reclaimed
     *  underneath it. The pool must belong to this broker alone: a second strategy over the same
     *  accounts keeps its own books — see "Limits" in the README. */
    public AccountBroker(List<Account> accounts, long leaseTtlMillis, long acquireTimeoutMillis) {
        this(accounts, leaseTtlMillis, acquireTimeoutMillis, System::nanoTime);
    }

    /** Lets a test supply its own clock so a lease can be expired instantly. It drives expiry only;
     *  the acquire deadline stays on the real clock, so a frozen clock cannot hang the suite. */
    AccountBroker(List<Account> accounts, long leaseTtlMillis, long acquireTimeoutMillis,
            LongSupplier nanoTime) {
        if (accounts.isEmpty() || leaseTtlMillis <= 0 || acquireTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, a positive ttl and a non-negative timeout");
        }
        // Leases are keyed by id: a duplicate would strand a live lease that could never expire.
        if (accounts.stream().map(Account::id).distinct().count() != accounts.size()) {
            throw new IllegalArgumentException("account ids must be unique");
        }
        this.accounts = List.copyOf(accounts);
        // From the copy: the caller's own list can still change between these two statements.
        this.capabilities = new TreeSet<>(this.accounts.stream().map(Account::capability).toList());
        this.leaseTtlNanos = TimeUnit.MILLISECONDS.toNanos(leaseTtlMillis);
        this.acquireTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        this.nanoTime = nanoTime;
    }

    @Override
    public Lease acquire(String owner) throws InterruptedException {
        return acquire(owner, null);
    }

    /** Lease any free account tagged {@code capability} ({@code null} matches anything) — asking for
     *  what you need rather than which account is what keeps the whole pool eligible.
     *
     *  @throws IllegalArgumentException if no account in the pool carries {@code capability}
     *  @throws NoAccountAvailableException if nothing frees up within the acquire timeout */
    public Lease acquire(String owner, String capability) throws InterruptedException {
        // Before any waiting: this is the whole difference between a typo and a timeout.
        if (capability != null && !capabilities.contains(capability)) {
            throw new IllegalArgumentException("no account in the pool has capability '" + capability
                    + "'; the pool offers " + capabilities);
        }
        // Real clock, not the injected one: a frozen test clock must not turn a timeout into a hang.
        long start = System.nanoTime();
        long deadline = start + acquireTimeoutNanos;
        lock.lock();
        try {
            while (true) {
                // On the acquiring thread: a waiter is exactly who cares. No background thread.
                reclaimExpiredLeases();
                for (Account account : accounts) {
                    if (account.hasCapability(capability) && account.tryHold(owner)) {
                        Lease lease = new Lease(this, account, owner);
                        held.put(account.id(), new Held(lease, nanoTime.getAsLong() + leaseTtlNanos));
                        return lease;
                    }
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new NoAccountAvailableException(owner, capability,
                            Duration.ofNanos(System.nanoTime() - start));
                }
                accountReleased.await(Math.min(remaining, RECLAIM_POLL_NANOS), TimeUnit.NANOSECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Conditional on <em>this exact lease</em>: an expired one may already have been re-issued,
     *  possibly to the same owner, so releasing on the label alone would rob the live holder. */
    @Override
    public void release(Lease lease) {
        lock.lock();
        try {
            Held current = held.get(lease.account().id());
            if (current != null && current.lease() == lease) {
                held.remove(lease.account().id());
                lease.account().forceRelease();
                // Waiters want different capabilities, so only they can tell whether this helps.
                accountReleased.signalAll();
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

    /** Frees leases whose owner is gone: crashed process, killed CI job, forgotten teardown. */
    private void reclaimExpiredLeases() {
        long now = nanoTime.getAsLong();
        for (Account account : accounts) {
            Held current = held.get(account.id());
            if (current != null && current.expiresAt() - now <= 0) {
                held.remove(account.id());
                account.forceRelease();
                reclaimedLeases++;
            }
        }
    }
}
