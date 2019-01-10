package org.qcri.rheem.experiment.word2nvec;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.experiment.implementations.rheem.RheemImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;

final public class Word2NVecRheemImplementation extends RheemImplementation {
    public Word2NVecRheemImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter)parameters.getParameter("input")).getPath();
        String output = ((FileResult)results.getContainerOfResult("output")).getPath();
        int minWordOccurrences = ((VariableParameter<Integer>)parameters.getParameter("min")).getVariable();

        TextFileSource source = new TextFileSource(input);
        source.setName("read corpus[1]");

        FlatMapOperator<String, String> split = new FlatMapOperator<>(
                line -> {
                    String[] words = line.split("\\W+");
                    ArrayList<String> list = new ArrayList<>(words.length);
                    for(String word: words){
                        if(!word.isEmpty()){
                            list.add(word);
                        }
                    }
                    return list;
                },
                String.class,
                String.class
        );
        split.setName("Split & scrub");


        MapOperator<String, Tuple2<String, Integer>> addCounter = new MapOperator<String, Tuple2<String, Integer>>(
            word -> new Tuple2(word, 1),
            String.class,
            ReflectionUtils.specify(Tuple2.class)
        );
        addCounter.setName("add Counter");


        ReduceByOperator<Tuple2<String, Integer>, String> sum_words = new ReduceByOperator<>(
            tuple -> tuple.field0,
            (tuple1, tuple2) -> {
                tuple1.field1 += tuple2.field1;
                return tuple1;
            },
            String.class,
            ReflectionUtils.specify(Tuple2.class)
        );

        FilterOperator<Tuple2<String, Integer>> frecuence = new FilterOperator<>(
            tuple -> tuple.field1 >= minWordOccurrences,
            ReflectionUtils.specify(Tuple2.class)
        );

        MapOperator<Tuple2<String, Integer>, String> toWord = new MapOperator<>(
            tuple -> tuple.field0,
            ReflectionUtils.specify(Tuple2.class),
            String.class
        );

        ZipWithIdOperator<String> zipWithIdOperator = new ZipWithIdOperator<>(String.class);

        MapOperator<Tuple2<Long, String>, Tuple2<String, Integer>> convert = new MapOperator<>(
                tuple -> new Tuple2<>(tuple.field1, tuple.field0.intValue()),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class)
        );


        source.connectTo(0, split, 0);
        split.connectTo(0, addCounter, 0);
        addCounter.connectTo(0, sum_words, 0);
        sum_words.connectTo(0, frecuence, 0);
        frecuence.connectTo(0, toWord, 0);
        toWord.connectTo(0, zipWithIdOperator, 0);
        zipWithIdOperator.connectTo(0, convert, 0);





    }
}
