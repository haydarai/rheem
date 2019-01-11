package org.qcri.rheem.experiment;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.qcri.rheem.experiment.crocopr.Crocopr;
import org.qcri.rheem.experiment.kmeans.Kmeans;
import org.qcri.rheem.experiment.sgd.SGD;
import org.qcri.rheem.experiment.simwords.SimWords;
import org.qcri.rheem.experiment.tpch.TPCH;
import org.qcri.rheem.experiment.word2nvec.Word2NVec;
import org.qcri.rheem.experiment.wordcount.WordCount;
import org.qcri.rheem.experiment.utils.results.RheemResults;

import java.util.HashMap;

public class ExperimentExecutor extends ExperimentController {

    private HashMap<String, RheemExperiment> experiments;

    private Options opts;

    public ExperimentExecutor(String... args) {
        super(args);
        experiments = new HashMap<>();
        experiments.put("crocopr", new Crocopr());
        experiments.put("kmeans", new Kmeans());
        experiments.put("sgd", new SGD());
        experiments.put("simwords", new SimWords());
        experiments.put("tpch", new TPCH());
        experiments.put("word2nvec", new Word2NVec());
        experiments.put("wordcount", new WordCount());
    }

    public static void main(String... args){
        ExperimentExecutor executor =  new ExperimentExecutor(args);

        String name_experiment = executor.getValue("exn").toLowerCase();

        if(executor.experiments.containsKey(name_experiment)){
            RheemExperiment experiment = executor.experiments.get(name_experiment);
            experiment.addOptions(executor.opts);
            executor.generateCommandLine();
            Implementation implementation = experiment.buildImplementation(executor);
            long start = System.currentTimeMillis();
            RheemResults results = implementation.executePlan();
            long finish = System.currentTimeMillis();
            System.err.println(
                String.format(
                    "the time for the experiment %s in the platform %s was %d ms",
                    name_experiment,
                    executor.getValue("platform"),
                    (finish - start)
                )
            );
            //  results.show();
        }else{
            throw new ExperimentException(
                String.format(
                    "the experiment %s not exist in the pool of possible experiments %s",
                    name_experiment,
                    executor.experiments.keySet()
                )
            );
        }


    }


    @Override
    protected Options buildOptions() {
        opts = new Options();

        Option exp_name_option = new Option(
                "exn",
                "experiment-name",
                true,
                "The name of the experiment that will run"
        );
        exp_name_option.setRequired(true);
        opts.addOption(exp_name_option);

        Option plat_option = new Option(
                "plat",
                "platform",
                true,
                "Define the platform where the experiment will be executed"
        );
        plat_option.setRequired(true);
        opts.addOption(plat_option);

        return opts;
    }

    @Override
    protected String isValid() {
        return null;
    }

}
