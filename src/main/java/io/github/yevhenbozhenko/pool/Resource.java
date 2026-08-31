package io.github.yevhenbozhenko.pool;

import java.util.Objects;
import java.util.Set;

/** One shareable thing: a stable id, the tags callers select on, and {@code payload} — whatever the
 *  caller actually needs once it holds it (a login, a JDBC url, a device handle). Immutable, so a
 *  pool can hand it out without a caller being able to corrupt the pool's books.
 *
 *  <p>Tags are free-form strings this library never interprets. That is deliberate: the moment the
 *  set of allowed values is written down here, the library belongs to one domain.
 *
 *  <p>{@code payload} may be null — a pool of plain permits is a {@code Resource<Void>}. The id
 *  and tags may not.
 *
 *  @param <T> what the caller gets back from {@link Lease#get()} */
public record Resource<T>(String id, Set<String> tags, T payload) {

    public Resource {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tags, "tags");
        // Copied: the caller's own set can still change afterwards.
        tags = Set.copyOf(tags);
    }

    /** An untagged resource, for a pool whose members are interchangeable. */
    public Resource(String id, T payload) {
        this(id, Set.of(), payload);
    }
}
