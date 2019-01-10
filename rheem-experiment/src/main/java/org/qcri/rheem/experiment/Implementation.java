package org.qcri.rheem.experiment;

import org.qcri.rheem.experiment.enviroment.EnviromentExecution;
import org.qcri.rheem.experiment.enviroment.FlinkEnviroment;
import org.qcri.rheem.experiment.enviroment.JavaEnviroment;
import org.qcri.rheem.experiment.enviroment.RheemEnviroment;
import org.qcri.rheem.experiment.enviroment.SparkEnviroment;
import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.results.RheemResults;
import org.qcri.rheem.utils.results.type.RheemResult;
import org.qcri.rheem.utils.udf.UDFs;

import java.io.Serializable;

public abstract class Implementation implements Serializable {

    public static final String FLINK = "flink";
    public static final String SPARK = "spark";
    public static final String JAVA  = "java";
    public static final String RHEEM = "rheem";


    protected final String platform;
    protected EnviromentExecution enviroment;
    protected RheemParameters parameters;
    protected RheemResults results;
    protected UDFs udfs;

    public Implementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        if(validate(platform)) {
            this.platform = platform;
        }else{
            throw new ExperimentException(
                String.format(
                    "The platform %s is not valid with the possibles options [%s, %s, %s, %s]",
                    platform,
                    FLINK,
                    SPARK,
                    JAVA,
                    RHEEM
                )
            );
        }
        this.parameters = parameters;
        this.results = result;
        this.udfs = udfs;
    }

    private boolean validate(String name_platform){
        switch (name_platform.toLowerCase()) {
            case FLINK:
                this.enviroment = new FlinkEnviroment();
                return true;
            case SPARK:
                this.enviroment = new SparkEnviroment();
                return true;
            case JAVA :
                this.enviroment = new JavaEnviroment();
                return true;
            case RHEEM:
                this.enviroment = new RheemEnviroment();
                return true;
            default:
                return false;
        }
    }

    public abstract RheemResults executePlan();

}
