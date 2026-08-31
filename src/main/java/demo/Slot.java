package demo;

import io.github.yevhenbozhenko.pool.Resource;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Holder bookkeeping for the two flawed strategies. {@code LeaseBroker} keeps the same state
 *  privately behind its lock; here it is reachable, because that is the lesson. */
final class Slot<T> {

    final Resource<T> resource;
    private final AtomicReference<String> holder = new AtomicReference<>();

    Slot(Resource<T> resource) {
        this.resource = Objects.requireNonNull(resource, "resource");
    }

    boolean isInUse() {
        return holder.get() != null;
    }

    /** UNSAFE: overwrites whoever holds it. Only correct where something else stops two callers
     *  arriving at once, or where the race is the point (strategy 1). */
    void forceHold(String owner) {
        holder.set(owner);
    }

    /** UNSAFE: frees the resource no matter who holds it. */
    void forceRelease() {
        holder.set(null);
    }

    /** Not {@code compareAndSet(owner, null)}: that compares the names as objects rather than as
     *  text, and equal names built separately would silently fail to match. */
    boolean releaseIfHeldBy(String owner) {
        String current = holder.get();
        return owner.equals(current) && holder.compareAndSet(current, null);
    }
}
