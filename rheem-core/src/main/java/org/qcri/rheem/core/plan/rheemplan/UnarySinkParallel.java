package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Created by bertty on 22-11-17.
 */
public abstract class UnarySinkParallel<T> extends UnarySink<T> {

    public UnarySinkParallel(DataSetType<T> type) {
        super(type);
    }

    public UnarySinkParallel(DataSetType<T> type, boolean isSupportingBroadcastInputs) {
        super(type, isSupportingBroadcastInputs);
    }

    public UnarySinkParallel(UnarySink<T> that) {
        super(that);
    }
}
