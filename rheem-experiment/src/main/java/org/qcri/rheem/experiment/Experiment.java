package org.qcri.rheem.experiment;

import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.results.RheemResults;

public interface Experiment {

    void implementPlan(RheemParameters parameters);

    RheemResults executePlan();

}
