package org.qcri.rheem.experiment.wordcount;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.ExperimentController;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.RheemExperiment;
import org.qcri.rheem.experiment.wordcount.flink.FlinkImplementation;
import org.qcri.rheem.experiment.wordcount.java.JavaImplementation;
import org.qcri.rheem.experiment.wordcount.rheem.RheemImplementation;
import org.qcri.rheem.experiment.wordcount.spark.SparkImplementation;
import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.parameters.type.FileParameter;
import org.qcri.rheem.utils.results.RheemResults;
import org.qcri.rheem.utils.results.type.FileResult;
import org.qcri.rheem.utils.udf.UDFs;

public class WordCount extends RheemExperiment {

    public WordCount() {
        super();
    }

    @Override
    public void addOptions(Options options) {
        Option input_file_option = new Option(
            "i",
                "input_file",
                true,
                "The location of the input file that will use in the Wordcount"
        );
        input_file_option.setRequired(true);
        options.addOption(input_file_option);

        Option output_file_option = new Option(
                "o",
                "output_file",
                true,
                "the location of the output file that will use for the wordcount"
        );
        output_file_option.setRequired(true);
        options.addOption(output_file_option);
    }

    @Override
    public Implementation buildImplementation(ExperimentController controller) {
        String platform = controller.getValue("plat").toLowerCase();
        RheemParameters parameters = new RheemParameters();

        parameters.addParameter("input", new FileParameter(controller.getValue("input_file")));

        RheemResults results = new RheemResults();

        results.addContainerOfResult("output", new FileResult(controller.getValue("output_file")));

        UDFs udfs = new UDFs();
        //todo add the UDFS


        Implementation implementation;
        switch (platform){
            case Implementation.FLINK:
                implementation = new FlinkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.SPARK:
                implementation = new SparkImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.JAVA:
                implementation = new JavaImplementation(platform, parameters, results, udfs);
                break;
            case Implementation.RHEEM:
                implementation = new RheemImplementation(platform, parameters, results, udfs);
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
