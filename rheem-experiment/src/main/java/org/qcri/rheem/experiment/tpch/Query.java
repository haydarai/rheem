package org.qcri.rheem.experiment.tpch;

import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentExecutor;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;

public abstract class Query {

    public Query(ExperimentController executor){
        this.addOptions(executor.getOptions());
        executor.generateCommandLine();
    }



    public abstract void addOptions(Options options);
    public abstract Implementation buildImplementation(ExperimentController controller, RheemParameters parameters, RheemResults results);
}
