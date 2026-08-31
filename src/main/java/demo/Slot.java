package demo;

import io.github.yevhenbozhenko.pool.Resource;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Holder bookkeeping for the two flawed strategies. {@code LeaseBroker} keeps the same thing
 *  privately, behind its lock; here it is deliberately reachable, because what strategy 1 does with
 *  an unprotected holder is the lesson. */
final class Slot<T> {

    final Resource<T> resource;
    private final AtomicReference<String> holder = new AtomicReference<>();

    Slot(Resource<T> resource) {
        // Both strategies build slots here, so a null pool entry is caught now rather than later,
        // inside an acquire on some worker thread.
        this.resource = Objects.requireNonNull(resource, "resource");
    }

    boolean isInUse() {
        return holder.get() != null;
    }

    /** UNSAFE: overwrites whoever holds it. Only correct where something else already stops two
     *  callers arriving at once, or where the race is the point (strategy 1). */
    void forceHold(String owner) {
        holder.set(owner);
    }

    /** UNSAFE: frees the resource no matter who holds it. */
    void forceRelease() {
        holder.set(null);
    }

    /** Reads the holder, then clears it. The shorter {@code compareAndSet(owner, null)} compares
     *  the two names as objects rather than as text, so two equal names built separately — which is
     *  how they are always built here — would silently fail to match. */
    boolean releaseIfHeldBy(String owner) {
        String current = holder.get();
        return owner.equals(current) && holder.compareAndSet(current, null);
    }
}
