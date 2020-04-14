package org.qcri.rheem.jena.mapping;

import org.qcri.rheem.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

public class Mappings {

    public static final Collection<Mapping> ALL = Arrays.asList(
            new ProjectionMapping(),
            new FilterMapping(),
            new JoinMapping()
    );
}
