package org.qcri.rheem.experiment.implementations.java;

import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public abstract class JavaImplementation extends Implementation {
    public JavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void preExecutePlan() {
        System.out.println("Implementation java");
    }

    @Override
    protected RheemResults postExecutePlan() {
        return this.results;
    }
}
