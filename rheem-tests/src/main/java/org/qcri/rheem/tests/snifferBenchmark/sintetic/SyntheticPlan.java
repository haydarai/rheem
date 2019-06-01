package org.qcri.rheem.tests.snifferBenchmark.sintetic;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.tests.snifferBenchmark.SnifferBenchmarkBase;

import java.util.Arrays;

public class SyntheticPlan extends SnifferBenchmarkBase {

    private String[] args;
    private int n_sniffers;
    private int n_operators;

    public SyntheticPlan(int n_operators, int n_sniffers, String... args){
        super();
        this.args = args;
        this.n_sniffers = n_sniffers;
        this.n_operators = n_operators;
    }

    @Override
    protected Operator[] generateBasePlan() {
        Operator[] operators = new Operator[this.n_operators];

        operators[0] = new TextFileSource(args[0]);
        operators[0].setName("Load file");
        for(int i = 1; i< operators.length -1 ; i++) {
            // for each line (input) output an iterator of the words
            operators[i] = new MapOperator<>(
                line -> line + "ddddd ",
                String.class,
                String.class
            );
        }
        // write results to a sink
        operators[this.n_operators - 1] = new TextFileSink<String>(
                args[1],
                tuple -> tuple.toString(),
                String.class
        );
        return operators;
    }
}
