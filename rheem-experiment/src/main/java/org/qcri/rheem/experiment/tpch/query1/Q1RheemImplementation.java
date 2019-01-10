package org.qcri.rheem.experiment.tpch.query1;

import org.qcri.rheem.experiment.implementations.rheem.RheemImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

final public class Q1RheemImplementation extends RheemImplementation {
    public Q1RheemImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {

    }
}
