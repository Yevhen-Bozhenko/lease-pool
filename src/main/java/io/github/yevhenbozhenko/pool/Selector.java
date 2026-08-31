package io.github.yevhenbozhenko.pool;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** What a caller will accept, expressed over a resource's tags rather than its id — asking for what
 *  you need instead of naming one thing keeps the whole pool eligible.
 *
 *  <p>A selector must be a pure function of the tag set. Pools evaluate it once up front to tell an
 *  unsatisfiable request from a busy one, so a selector that changes its mind turns that instant
 *  failure into a silent wait.
 *
 *  <p>Give a hand-written selector a {@code toString()}: both failure messages print the selector
 *  to say what was asked for, and a lambda inherits one that names nothing. */
@FunctionalInterface
public interface Selector {

    boolean matches(Set<String> tags);

    /** Any resource in the pool will do. */
    static Selector any() {
        return TaggedSelector.ANY;
    }

    /** Matches a resource carrying <em>all</em> of {@code required}. Extra tags are fine, so adding
     *  a tag to a resource can never stop an existing caller from matching it. */
    static Selector tagged(String... required) {
        return required.length == 0 ? TaggedSelector.ANY
                : new TaggedSelector(Set.copyOf(List.of(required)));
    }
}

/** Package-private so {@link Selector#tagged} stays the only way to build one, and so the diagnostic
 *  a failed acquire prints reads as tags rather than as a class name. */
record TaggedSelector(Set<String> required) implements Selector {

    static final TaggedSelector ANY = new TaggedSelector(Set.of());

    TaggedSelector {
        required = Set.copyOf(required);
    }

    @Override
    public boolean matches(Set<String> tags) {
        return tags.containsAll(required);
    }

    @Override
    public String toString() {
        return required.isEmpty() ? "any resource" : "tags " + new TreeSet<>(required);
    }
}
