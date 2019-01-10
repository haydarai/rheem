package org.qcri.rheem.experiment.word2nvec;

import org.qcri.rheem.experiment.implementations.java.JavaImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class Word2NVecJavaImplementation extends JavaImplementation {
    public Word2NVecJavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {

    }
}
