package org.qcri.rheem.experiment.crocopr;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.RheemExperiment;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class Crocopr extends RheemExperiment {

    public Crocopr() {
        super();
    }

    @Override
    public void addOptions(Options options) {
        Option input_file_option = new Option(
                "i",
                "input_file",
                true,
                "The location of the input file that will use in the Crocopr"
        );
        input_file_option.setRequired(true);
        options.addOption(input_file_option);

        Option input2_file_option = new Option(
                "i2",
                "input2_file",
                true,
                "The location of the input2 file that will use in the Crocopr"
        );
        input2_file_option.setRequired(true);
        options.addOption(input2_file_option);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the Crocopr"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {
        String platform = controller.getValue("plat").toLowerCase();
        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("input", new FileParameter(controller.getValue("input_file")));
        parameters.addParameter("input2", new FileParameter(controller.getValue("input2_file")));

        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new CrocoprFlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new CrocoprSparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new CrocoprJavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new CrocoprRheemImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.GIRAPH:
                implementation = new CrocoprGiraphImplementation(platform, parameters, results, udfs);
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
