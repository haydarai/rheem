package org.qcri.rheem.experiment.word2nvec;

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

public class Word2NVec extends RheemExperiment{

    public Word2NVec() {
        super();
    }

    @Override
    public void addOptions(Options options) {
        Option input_file_option = new Option(
                "i",
                "input_file",
                true,
                "The location of the input file that will use in the Word2NVec"
        );
        input_file_option.setRequired(true);
        options.addOption(input_file_option);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the Word2NVec"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);


        Option min_occurs_option = new Option(
                "min",
                "minWordOccurrences",
                true,
                "min Word Occurrences for use as parameters in Word2NVec"
        );
        output_file_option.setRequired(true);
        options.addOption(min_occurs_option);

        Option neighborhoodReach_option = new Option(
                "nr",
                "neighborhood-reach",
                true,
                "min Word Occurrences for use as parameters in Word2NVec"
        );
        neighborhoodReach_option.setRequired(true);
        options.addOption(neighborhoodReach_option);

    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {
        String platform = controller.getValue("plat").toLowerCase();
        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("input", new FileParameter(controller.getValue("input_file")));

        parameters.addParameter("min", new VariableParameter<Integer>(controller.getIntValue("minWordOccurrences")));
        parameters.addParameter("neighborhoodReach", new VariableParameter<Integer>(controller.getIntValue("nr")));

        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        UDFs udfs = new UDFs();

        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new Word2NVecFlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new Word2NVecSparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new Word2NVecJavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new Word2NVecRheemImplementation(platform, parameters, results, udfs);
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
