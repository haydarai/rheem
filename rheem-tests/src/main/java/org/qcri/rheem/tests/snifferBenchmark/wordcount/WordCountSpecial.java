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
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemUUID;
import org.qcri.rheem.tests.snifferBenchmark.SnifferBenchmarkBase;

import java.util.ArrayList;
import java.util.Arrays;

public class WordCountSpecial extends SnifferBenchmarkBase {
    private String[] args;
    private int n_sniffers;

    public WordCountSpecial(int n_sniffers, String... args) {
        super();
        this.args = args;
        this.n_sniffers = n_sniffers;
    }


    @Override
    protected Operator[] generateBasePlan() {
        Class type = String.class;
        ArrayList<Operator> operators = new ArrayList<>();
        int index = 0;
        operators.add( new TextFileSource(args[0]) );
        operators.get(index).setName("Load file");
        index++;

        operators.add( new MapOperator<>(
                word -> word,
                String.class,
                String.class
            )
        );
        operators.get(index).setName("identity");
        index++;

        // for each line (input) output an iterator of the words
        operators.add( new FlatMapOperator<>(
                line -> Arrays.asList(line.split("\\W+")),
                String.class,
                String.class
            )
        );
        operators.get(index).setName("Split words");
        index++;

        operators.add( new FilterOperator<>(str -> !str.isEmpty(), String.class) );
        operators.get(index).setName("Filter empty words");
        index++;

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        operators.add( new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
            )
        );
        operators.get(index).setName("To lower case, add counter");
        type = Tuple2.class;
        index++;

        // groupby the key (word) and add up the values (frequency)
        final RheemUUID one = RheemUUID.randomUUID();
        final RheemUUID two = RheemUUID.randomUUID();
        operators.add(
            new ReduceByOperator<Tuple2<String, Integer>, String>(
                new TransformationDescriptor<>(
                    pair -> pair.field0,
                    DataUnitType.createBasicUnchecked(Tuple2.class),
                    DataUnitType.createBasic(String.class)
                ),
                new ReduceDescriptor<>(
                    ((a, b) -> {
                       /* one.tobyte();
                        two.tobyte();
                        one.bytes = null;
                        two.bytes = null;*/
                        a.field1 += b.field1;
                        return a;
                    }),
                    DataUnitType.createGroupedUnchecked(Tuple2.class),
                    DataUnitType.createBasicUnchecked(Tuple2.class)
                ),
                DataSetType.createDefaultUnchecked(Tuple2.class)
            )
        );
        operators.get(index).setName("Add counters");
        index++;

        // write results to a sink
        //Class

        operators.add( new FilterOperator<>(element -> false, type) );
        operators.get(index).setName("cleaner");
        index++;

        operators.add( new TextFileSink<>(
                args[1],
                tuple -> tuple.toString(),
                type
            )
        );
        operators.get(index).setName("Collect result");

        return operators.toArray(new Operator[0]);
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
