package io.github.yevhenbozhenko.pool;

import java.time.Duration;

/** Saturation: every resource that could have served the request was held until the acquire timeout
 *  ran out. Worth retrying — unlike the {@link IllegalArgumentException} from a selector nothing in
 *  the pool matches, which no amount of waiting will fix. Carries the three facts a caller needs to
 *  decide, so it does not have to parse {@link #getMessage()}. */
public final class NoResourceAvailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String owner;
    private final transient Selector selector;
    private final Duration waited;

    public NoResourceAvailableException(String owner, Selector selector, Duration waited) {
        super(owner + " waited " + waited.toMillis() + " ms for " + selector + " and got none");
        this.owner = owner;
        this.selector = selector;
        this.waited = waited;
    }

    public String owner() {
        return owner;
    }

    /** What the caller asked for. {@code null} on an exception that crossed a serialisation
     *  boundary, since a selector may be a lambda. */
    public Selector selector() {
        return selector;
    }

    /** How long the caller actually blocked — at least the acquire timeout, never less. */
    public Duration waited() {
        return waited;
    }
}
