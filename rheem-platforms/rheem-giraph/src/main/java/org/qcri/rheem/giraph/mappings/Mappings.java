package org.qcri.rheem.giraph.mappings;

import org.qcri.rheem.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Register for {@link Mapping}s for this platform.
 */
public class Mappings {

    public static final Collection<Mapping> ALL = Arrays.asList(
            new PageRankMapping(),
            new ShortestPathMapping()
    );
}
