package io.github.yevhenbozhenko.pool;

/** A pool that hands out resources one holder at a time. The whole public surface is acquire and
 *  release; how exclusion is achieved is the implementation's business.
 *
 *  @param <T> the payload type the pool's resources carry */
public interface ResourcePool<T> {

    /** Blocks until any resource is free. Concurrent holders must not share an {@code owner}. */
    default Lease<T> acquire(String owner) throws InterruptedException {
        return acquire(owner, Selector.any());
    }

    /** Blocks until a resource matching {@code selector} is free. */
    Lease<T> acquire(String owner, Selector selector) throws InterruptedException;

    /** Must tolerate a <em>stale</em> lease whose resource was already reclaimed, and must not free
     *  a resource now held by someone else. Prefer closing the lease. */
    void release(Lease<T> lease);
}
