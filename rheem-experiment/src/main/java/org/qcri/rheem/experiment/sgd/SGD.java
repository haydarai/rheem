package org.qcri.rheem.experiment.sgd;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.RheemExperiment;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

public class SGD extends RheemExperiment {

    public SGD() {
        super();
    }

    @Override
    public void addOptions(Options options) {
        Option input_file_option = new Option(
                "i",
                "input_file",
                true,
                "The location of the input file that will use in the SGD"
        );
        input_file_option.setRequired(true);
        options.addOption(input_file_option);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the SGD"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);

        Option features_option = new Option(
                "f",
                "features",
                true,
                "the numbers of features"
        );
        features_option.setRequired(true);
        options.addOption(features_option);

        Option sample_size_option = new Option(
                "s",
                "sample_size",
                true,
                "the sample size in all the iterations"
        );
        sample_size_option.setRequired(true);
        options.addOption(sample_size_option);


        Option max_iterations_option = new Option(
                "mi",
                "max_iterations",
                true,
                "the number of maximum iteration"
        );
        max_iterations_option.setRequired(true);
        options.addOption(max_iterations_option);


        Option accuracy_option = new Option(
                "a",
                "accuracy",
                true,
                "the accuracy of the weights"
        );
        accuracy_option.setRequired(true);
        options.addOption(accuracy_option);
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {
        String platform = controller.getValue("plat").toLowerCase();
        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("input", new FileParameter(controller.getValue("input_file")));
        parameters.addParameter("features", new VariableParameter<Integer>(controller.getIntValue("features")));
        parameters.addParameter("sample_size", new VariableParameter<Integer>(controller.getIntValue("sample_size")));
        parameters.addParameter("max_iterations", new VariableParameter<Integer>(controller.getIntValue("max_iterations")));
        parameters.addParameter("accuracy", new VariableParameter<Double>(controller.getDoubleValue("accuracy")));


        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new SGDFlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new SGDSparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new SGDJavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new SGDRheemImplementation(platform, parameters, results, udfs);
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
