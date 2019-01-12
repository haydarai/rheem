package org.qcri.rheem.experiment.tpch.query3;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.tpch.Query;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class Q3 extends Query {

    public Q3(ExperimentController controller) {
        super(controller);
    }

    @Override
    public void addOptions(Options options) {
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller, RheemParameters parameters, RheemResults results) {
        String platform = controller.getValue("plat").toLowerCase();

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new Q3FlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new Q3SparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new Q3JavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new Q3RheemImplementation(platform, parameters, results, udfs);
                break;
            default:
                throw new ExperimentException(
                        String.format(
                                "The platform %s it's not valid for experiment %s",
                                platform,
                                this.getClass().getSimpleName()
                        )
                );
        }

        return implementation;
    }
}
