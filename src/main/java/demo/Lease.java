package demo;

import java.util.concurrent.atomic.AtomicBoolean;

/** An owner's claim on an {@link Account}, carrying the one thing a bare {@code release(account)}
 *  throws away: <em>who</em> is releasing. Closing twice is harmless, so calling {@code close()}
 *  yourself inside a try-with-resources block cannot release the account twice. */
public final class Lease implements AutoCloseable {

    private final AccountStrategy origin;
    private final Account account;
    private final String owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Package-private: only a strategy may issue a lease. */
    Lease(AccountStrategy origin, Account account, String owner) {
        this.origin = origin;
        this.account = account;
        this.owner = owner;
    }

    public Account account() {
        return account;
    }

    public String owner() {
        return owner;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            origin.release(this);
        }
    }
}
