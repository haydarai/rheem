package org.qcri.rheem.experiment.wordcount;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.experiment.implementations.rheem.RheemImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.Arrays;

final public class WordCountRheemImplementation extends RheemImplementation {

    public WordCountRheemImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String inputFileUrl = ((FileParameter) parameters.getParameter("input")).getPath();
        TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.setName("Load file");

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                line -> Arrays.asList(line.split("\\W+")),
                String.class,
                String.class
        );
        flatMapOperator.setName("Split words");

        FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                word -> new Tuple2<>(word.toLowerCase(), 1),
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );
        mapOperator.setName("To lower case, add counter");


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                pair -> pair.field0,
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }),
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );
        reduceByOperator.setName("Add counters");

        String outputFileUrl = ((FileResult) this.results.getContainerOfResult("output")).getPath();
        // write results to a sink
        TextFileSink<Tuple2<String, Integer>> sink = new TextFileSink<Tuple2<String, Integer>>(
                outputFileUrl,
                ReflectionUtils.specify(Tuple2.class)
        );
        sink.setName("Saving result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        this.sinks.add(sink);
    }
}
