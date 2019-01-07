package org.qcri.rheem.experiment;

import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.results.RheemResults;
import org.qcri.rheem.utils.results.type.RheemResult;
import org.qcri.rheem.utils.udf.UDFs;

public abstract class Implementation {

    public static final String FLINK = "flink";
    public static final String SPARK = "spark";
    public static final String JAVA  = "java";
    public static final String RHEEM = "rheem";


    protected final String platform;
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
            case SPARK:
            case JAVA :
            case RHEEM:
                return true;
            default:
                return false;
        }
    }

    public abstract RheemResults executePlan();

}
