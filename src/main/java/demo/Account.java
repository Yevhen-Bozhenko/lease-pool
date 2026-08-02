package demo;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** One shareable account: stable id, opaque capability tag, thread-safe holder. Mutators are
 *  package-private — {@link Lease#account()} hands out live references, and mutating one behind
 *  {@link AccountBroker}'s back corrupts its books. */
public final class Account {

    private final String id;
    private final String capability;
    private final AtomicReference<String> holder = new AtomicReference<>();

    public Account(String id, String capability) {
        this.id = Objects.requireNonNull(id, "id");
        this.capability = Objects.requireNonNull(capability, "capability");
    }

    public String id() {
        return id;
    }

    public String capability() {
        return capability;
    }

    public String holder() {
        return holder.get();
    }

    public boolean isInUse() {
        return holder.get() != null;
    }

    /** {@code null} means "any account will do". */
    public boolean hasCapability(String requested) {
        return requested == null || capability.equals(requested);
    }

    public boolean isHeldBy(String owner) {
        return owner.equals(holder.get());
    }

    /** Claims the account only if it is free, in one uninterruptible step, so exactly one of several
     *  racing callers wins — the whole difference from {@link NaiveSharedList}. A null owner is
     *  rejected: replacing "nobody" with "nobody" would report success without claiming anything. */
    boolean tryHold(String owner) {
        return holder.compareAndSet(null, Objects.requireNonNull(owner, "owner"));
    }

    /** Reads the holder first, then clears it. The shorter {@code compareAndSet(owner, null)} would
     *  compare the two owner labels as objects rather than as text, so two equal names built
     *  separately — which is how they are always built here — would silently fail to match. */
    boolean releaseIfHeldBy(String owner) {
        String current = holder.get();
        return owner.equals(current) && holder.compareAndSet(current, null);
    }

    /** UNSAFE: overwrites whoever holds it, without checking. Only correct where something else
     *  already stops two callers arriving at once, or where the race is the point (strategy 1). */
    void forceHold(String owner) {
        holder.set(owner);
    }

    /** UNSAFE: frees the account no matter who holds it. Prefer {@link #releaseIfHeldBy}. */
    void forceRelease() {
        holder.set(null);
    }
}
