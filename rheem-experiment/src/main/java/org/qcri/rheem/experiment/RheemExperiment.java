package org.qcri.rheem.experiment;

import org.apache.commons.cli.Options;

public abstract class RheemExperiment {

    public RheemExperiment(){}

    public abstract void addOptions(Options options);

    public abstract Implementation buildImplementation(ExperimentController controller);

}
