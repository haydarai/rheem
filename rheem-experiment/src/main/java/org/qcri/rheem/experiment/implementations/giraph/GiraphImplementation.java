package org.qcri.rheem.experiment.implementations.giraph;

import org.apache.giraph.conf.GiraphConfiguration;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;


public abstract class GiraphImplementation extends Implementation {
    protected GiraphConfiguration conf;

    public GiraphImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void preExecutePlan() {
        System.out.println("Implementation Giraph");


    }

    @Override
    protected RheemResults postExecutePlan() {
        return this.results;
    }

}
