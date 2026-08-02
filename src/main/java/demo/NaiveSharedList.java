package demo;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** Strategy 1 — scan a shared list and take the first account that looks free. Nothing locks, so
 *  two callers can both see the same account free and both take it. The fastest row, and wrong. */
public final class NaiveSharedList implements AccountStrategy {

    /** A wedge guard, not a queueing policy: it bounds the spin, it does not turn it into a wait. */
    private static final long DEFAULT_SPIN_LIMIT_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private final List<Account> accounts;
    private final long raceWindowMillis;
    private final long spinLimitNanos;

    public NaiveSharedList(List<Account> accounts, long raceWindowMillis) {
        this(accounts, raceWindowMillis, DEFAULT_SPIN_LIMIT_MILLIS);
    }

    /** Lets a test shorten the limit so it can be reached without a 30 s wait. Milliseconds. */
    NaiveSharedList(List<Account> accounts, long raceWindowMillis, long spinLimitMillis) {
        // An empty pool would otherwise spin out the whole limit before naming the real mistake.
        if (accounts.isEmpty() || raceWindowMillis < 0 || spinLimitMillis <= 0) {
            throw new IllegalArgumentException(
                    "need a non-empty pool, a non-negative race window and a positive spin limit");
        }
        this.accounts = List.copyOf(accounts);
        this.raceWindowMillis = raceWindowMillis;
        this.spinLimitNanos = TimeUnit.MILLISECONDS.toNanos(spinLimitMillis);
    }

    /** No waiting and no fairness: the one concession is the spin limit, so a fully-held pool fails
     *  loudly instead of pinning a core until you kill the JVM.
     *
     *  @throws IllegalStateException if nothing looks free before the spin limit elapses */
    @Override
    public Lease acquire(String owner) throws InterruptedException {
        long deadline = System.nanoTime() + spinLimitNanos;
        while (true) {
            for (Account account : accounts) {
                if (!account.isInUse()) {  // (1) CHECK
                    widenRaceWindow();
                    account.forceHold(owner);   // (2) ACT — last writer wins, and every earlier
                    return new Lease(this, account, owner); //  "owner" is evicted mid-test
                }
            }
            // Reached only when a whole scan found nothing, so the winning path is unchanged.
            // Thread.yield() neither throws nor clears the flag, so the spin must check it itself.
            if (Thread.interrupted()) {
                throw new InterruptedException(owner + " interrupted while spinning for an account");
            }
            // Compared by subtraction so the check still holds when the timer counter wraps around.
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException(owner + " spun for "
                        + TimeUnit.NANOSECONDS.toMillis(spinLimitNanos)
                        + " ms without seeing a free account");
            }
            // Step aside rather than sleep: a 1 ms sleep really takes ~15 ms on Windows, which
            // would show up in the table as if accounts were scarce rather than as a timer quirk.
            Thread.yield();
        }
    }

    /** THE RACE WINDOW, between the check and the claim above: nothing stops another thread passing
     *  the same check on the same account. Widening it does not *cause* the bug — the scan races at
     *  0 too — it only makes the damage land identically on every run. */
    private void widenRaceWindow() throws InterruptedException {
        Thread.sleep(raceWindowMillis);
    }

    /** Frees the account without checking we still hold it — a race loser frees the winner's. */
    @Override
    public void release(Lease lease) {
        lease.account().forceRelease();
    }
}
