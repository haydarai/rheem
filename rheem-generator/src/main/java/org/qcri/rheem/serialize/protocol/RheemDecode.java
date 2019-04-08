package org.qcri.rheem.serialize.protocol;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.RheemSerialized;

public interface RheemDecode<Protocol> {

    RheemPlan decode(RheemSerialized<Protocol> protocol);
}
