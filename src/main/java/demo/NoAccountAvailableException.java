package demo;

import java.time.Duration;

/** Saturation: every account that could have served the request was held until the acquire timeout
 *  ran out. Worth retrying — unlike the {@link IllegalArgumentException} from an unknown capability,
 *  which no amount of waiting will fix. Carries the three facts a caller needs to decide, so it does
 *  not have to parse {@link #getMessage()}. */
public final class NoAccountAvailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String owner;
    private final String capability;
    private final Duration waited;

    /** Package-private: only a strategy can know that a wait genuinely ran out. */
    NoAccountAvailableException(String owner, String capability, Duration waited) {
        super(owner + " waited " + waited.toMillis() + " ms for "
                + (capability == null ? "any free account" : "a free '" + capability + "' account")
                + " and got none");
        this.owner = owner;
        this.capability = capability;
        this.waited = waited;
    }

    public String owner() {
        return owner;
    }

    /** {@code null} when the caller asked for any account rather than a tagged one. */
    public String capability() {
        return capability;
    }

    /** How long the caller actually blocked — at least the acquire timeout, never less. */
    public Duration waited() {
        return waited;
    }
}
