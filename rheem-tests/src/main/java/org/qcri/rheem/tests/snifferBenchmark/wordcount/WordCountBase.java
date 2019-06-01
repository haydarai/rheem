package org.qcri.rheem.tests.snifferBenchmark.wordcount;

import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.tests.snifferBenchmark.SnifferBenchmarkBase;

import java.util.Arrays;

public class WordCountBase extends SnifferBenchmarkBase {

    private String[] args;
    private int n_sniffers;

    public WordCountBase(int n_sniffers, String... args){
        super();
        this.args = args;
        this.n_sniffers = n_sniffers;
    }


    @Override
    protected Operator[] generateBasePlan() {
        Operator[] operators = new Operator[6];
        operators[0] = new TextFileSource(args[0]);
        operators[0].setName("Load file");

        // for each line (input) output an iterator of the words
        operators[1] = new FlatMapOperator<>(
                line -> Arrays.asList(line.split("\\W+")),
                String.class,
                String.class
        );
        operators[1].setName("Split words");

        operators[2] = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        operators[2].setName("Filter empty words");


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        operators[3] = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        operators[3].setName("To lower case, add counter");

        // groupby the key (word) and add up the values (frequency)
        operators[4] = new ReduceByOperator<Tuple2<String, Integer>, String>(
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        ), DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        operators[4].setName("Add counters");

        // write results to a sink
        //Class type = Tuple2.class;
        Class type = Tuple2.class;
        operators[operators.length -1] = new TextFileSink<>(
                args[1],
                tuple -> tuple.toString(),
                type
        );
        operators[operators.length-1].setName("Collect result");

        return operators;
    }

    @Override
    protected Operator[] generateSniffers(int size){
        Operator[] op = new Operator[5];
        for(int i = 0; i < 3; i++) {
            op[i] = new SnifferOperator<String, Tuple1>(String.class, Tuple1.class).setFunction(
                    element -> {
                        return new Tuple1(null);
                    }
            );
        }
        for(int i = 3; i < 5; i++) {
            op[i] = new SnifferOperator<Tuple2, Tuple1>(Tuple2.class, Tuple1.class).setFunction(
                    element -> {
                        return new Tuple1(null);
                    }
            );
        }
        return Arrays.copyOfRange(op, 0, size);
    }
}
