package org.qcri.rheem.experiment.tpch.query1;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.tpch.Query;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class Q1 extends Query {

    public Q1(ExperimentController controller) {
        super(controller);
    }


    @Override
    public void addOptions(Options options) {
        Option start_day_option = new Option(
                "start",
                "start-date",
                true,
                "the date that will use for the filter, the format need be YYYY-MM-DD"
        );
        start_day_option.setRequired(true);
        options.addOption(start_day_option);

        Option delta_option = new Option(
                "d",
                "delta",
                true,
                "the difference in days that we will filter"
        );
        delta_option.setRequired(true);
        options.addOption(delta_option);
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller, RheemParameters parameters, RheemResults results) {
        String platform = controller.getValue("plat").toLowerCase();

        parameters.addParameter("date_min", new VariableParameter<String>(controller.getValue("start-date")));
        parameters.addParameter("delta", new VariableParameter<Integer>(controller.getIntValue("delta")));

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new Q1FlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new Q1SparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new Q1JavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new Q1RheemImplementation(platform, parameters, results, udfs);
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
