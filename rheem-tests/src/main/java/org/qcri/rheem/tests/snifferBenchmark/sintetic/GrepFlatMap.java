package org.qcri.rheem.tests.snifferBenchmark.sintetic;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.spark.compiler.debug.IteratorOneElement;

public class GrepFlatMap {
    public static void main(String... args){
        String inputFile = args[0];
        String outputFile = args[1];

        TextFileSource source = new TextFileSource(inputFile);


        FilterOperator<String> filter = new FilterOperator<String>(
                line -> {
                    line.contains("distinguished");
                    return true;
                },
                String.class
        );

        FlatMapOperator<String, String> flatMap = new FlatMapOperator<String, String>(
            line -> {
                return new IteratorOneElement<String>(line);
            },
            String.class,
            String.class
        );

        FilterOperator<String> filter_cleanr = new FilterOperator<String>(
                line -> {
                    return false;
                },
                String.class
        );

        TextFileSink<String> sink = new TextFileSink<String>(
                outputFile,
                record -> record.toString(),
                String.class
        );

        source.connectTo(0, filter, 0);
        filter.connectTo(0, flatMap, 0);
        flatMap.connectTo(0, filter_cleanr, 0);
        filter_cleanr.connectTo(0, sink, 0);

        GrepEmpty.execute(new RheemPlan(sink), GrepEmpty.class);
    }
}
