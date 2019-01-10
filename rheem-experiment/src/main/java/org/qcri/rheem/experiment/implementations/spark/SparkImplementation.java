package org.qcri.rheem.experiment.implementations.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.enviroment.SparkEnviroment;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public abstract class SparkImplementation extends Implementation {

    protected transient JavaSparkContext sparkContext;

    public SparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void preExecutePlan() {
        System.out.println("spark implementation");

        this.sparkContext = ((SparkEnviroment)this.enviroment).getEnviroment();
    }

    @Override
    protected RheemResults postExecutePlan() {
        return this.results;
    }
}
