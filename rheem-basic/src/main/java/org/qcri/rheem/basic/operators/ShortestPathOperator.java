package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

public class ShortestPathOperator extends UnaryToUnaryOperator<Tuple2<Long, Long>, Tuple2<Long, Float>> {

    public ShortestPathOperator() {
        super(DataSetType.createDefaultUnchecked(Tuple2.class), DataSetType.createDefaultUnchecked(Tuple2.class), false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ShortestPathOperator(ShortestPathOperator that) {
        super(that);
    }
}
