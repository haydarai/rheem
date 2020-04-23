package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * {@link Operator} for the DegreeCentrality algorithm. It takes as input a list of directed edges, whereby each edge
 * is represented as {@code (source vertex ID, target vertex ID)} tuple. Its output are the page ranks, codified
 * as {@code (vertex ID, page rank)} tuples.
 */
public class DegreeCentralityOperator extends UnaryToUnaryOperator<Tuple2<Long, Long>, Tuple2<Long, Integer>> {

    /**
     * Creates a new instance.
     *
     */
    public DegreeCentralityOperator() {
        super(DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public DegreeCentralityOperator(DegreeCentralityOperator that) {
        super(that);
    }

}
