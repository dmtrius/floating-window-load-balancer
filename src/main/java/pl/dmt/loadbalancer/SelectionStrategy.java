package pl.dmt.loadbalancer;

import java.util.List;

/**
 * Chooses one backend from the currently available candidates.
 *
 * <p>Implementations must be safe for concurrent calls and must return one of the supplied
 * candidates. The candidate list is immutable and never empty.
 */
@FunctionalInterface
public interface SelectionStrategy {

    Backend select(List<Backend> candidates);
}
