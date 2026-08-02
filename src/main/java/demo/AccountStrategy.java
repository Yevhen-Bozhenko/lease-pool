package demo;

/** How accounts are handed out to concurrent owners — the only thing the three implementations
 *  differ in, so {@link Benchmark} can attribute every difference in the table to the strategy. */
public interface AccountStrategy {

    /** Blocks until an account is free. Concurrent holders must not share an {@code owner} label. */
    Lease acquire(String owner) throws InterruptedException;

    /** Must tolerate a <em>stale</em> lease whose account was already TTL-reclaimed, and must not
     *  free an account now held by someone else. {@link NaiveSharedList} breaks this on purpose. */
    void release(Lease lease);
}
