package io.github.yevhenbozhenko.pool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** An owner's claim on a {@link Resource}, carrying the one thing a bare {@code release(resource)}
 *  throws away: <em>who</em> is releasing. Closing twice is harmless, so calling {@code close()}
 *  yourself inside a try-with-resources block cannot release the resource twice.
 *
 *  @param <T> the payload type of the leased resource */
public final class Lease<T> implements AutoCloseable {

    private final ResourcePool<T> origin;
    private final Resource<T> resource;
    private final String owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Public because {@link ResourcePool} is a public interface: anyone implementing a pool has to
     *  be able to issue a lease. Do not construct one outside a pool — its {@code close()} calls
     *  back into {@code origin}. */
    public Lease(ResourcePool<T> origin, Resource<T> resource, String owner) {
        this.origin = Objects.requireNonNull(origin, "origin");
        this.resource = Objects.requireNonNull(resource, "resource");
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    /** The thing you came for. */
    public T get() {
        return resource.payload();
    }

    public Resource<T> resource() {
        return resource;
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
