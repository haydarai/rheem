package org.qcri.rheem.serialize.protocol;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.RheemSerialized;

public interface RheemEncode<Protocol>{

    public RheemSerialized<Protocol> enconde(RheemPlan plan);
}
