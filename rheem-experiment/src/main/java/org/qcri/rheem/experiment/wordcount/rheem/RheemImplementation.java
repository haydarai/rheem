package org.qcri.rheem.experiment.wordcount.rheem;

import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.results.RheemResults;
import org.qcri.rheem.utils.udf.UDFs;

public class RheemImplementation extends Implementation {

    public RheemImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    public RheemResults executePlan() {
        System.out.println("rheem implementation");

        return null;
    }
}
