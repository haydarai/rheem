package org.qcri.rheem.experiment.implementations.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.enviroment.FlinkEnviroment;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public abstract class FlinkImplementation extends Implementation {

    protected ExecutionEnvironment env;
    public FlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void preExecutePlan() {
        System.out.println("Flink implementation");
        this.env = ((FlinkEnviroment)this.enviroment).getEnviroment();
    }

    @Override
    protected RheemResults postExecutePlan() {
        try {
            this.env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this.results;
    }
}
