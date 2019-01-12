package org.qcri.rheem.experiment.kmeans;

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

public class Kmeans extends RheemExperiment {

    public Kmeans() {
        super();
    }

    @Override
    public void addOptions(Options options) {
        Option input_file_option = new Option(
                "i",
                "input_file",
                true,
                "The location of the input file that will use in the Kmeans"
        );
        input_file_option.setRequired(true);
        options.addOption(input_file_option);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the Kmeans"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);

        Option n_centroid_option = new Option(
                "nc",
                "n_centroid",
                true,
                "number of centroids that will use for the Kmeans"
        );
        n_centroid_option.setRequired(true);
        options.addOption(n_centroid_option);

        Option seed_option = new Option(
                "s",
                "seed",
                true,
                "seed for the generation of the random centrodis"
        );
        seed_option.setRequired(true);
        options.addOption(seed_option);

        Option iterations_option = new Option(
                "iter",
                "iterations",
                true,
                "number of iteration for the kmeasn Kmeans"
        );
        iterations_option.setRequired(true);
        options.addOption(iterations_option);

    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {
        String platform = controller.getValue("plat").toLowerCase();
        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("input", new FileParameter(controller.getValue("input_file")));
        parameters.addParameter("n_centroid", new VariableParameter<Integer>(controller.getIntValue("n_centroid")));
        parameters.addParameter("seed", new VariableParameter<Long>(controller.getLongValue("seed")));
        parameters.addParameter("iterations", new VariableParameter<Integer>(controller.getIntValue("iterations")));

        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new KmeansFlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new KmeansSparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new KmeansJavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new KmeansRheemImplementation(platform, parameters, results, udfs);
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
