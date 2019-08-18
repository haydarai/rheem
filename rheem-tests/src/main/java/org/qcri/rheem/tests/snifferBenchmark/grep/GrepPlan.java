package org.qcri.rheem.tests.snifferBenchmark.grep;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.tests.snifferBenchmark.SnifferBenchmarkBase;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.HashMap;
import java.util.Map;

public class GrepPlan  extends SnifferBenchmarkBase {
    private Map<String, Operator> operatorPlan;
    private String inputFile;
    private String outputFile;

    private boolean sniffer;

    public GrepPlan(int n_sniffers, String input, String output){
        this.inputFile = input;
        this.outputFile = output;
        this.sniffer = n_sniffers > 0;
    }

    @Override
    protected Operator[] generateBasePlan() {
        this.operatorPlan = new HashMap<>();

        this.operatorPlan.put(
                "source",
                new TextFileSource(inputFile)
        );

        this.operatorPlan.put(
                "identity",
                new MapOperator<String, String>(
                        line -> line,
                        String.class,
                        String.class
                )
        );

        this.operatorPlan.put(
                "sniffer_1",
                new SnifferOperator<String, Tuple1>(String.class, Tuple1.class)
        );

        this.operatorPlan.put(
                "grep",
                new FilterOperator<String>(
                        line -> {
                            return line.contains("distinguished");
                        },
                        String.class
                )
        );

        this.operatorPlan.put(
                "sniffer_2",
                new SnifferOperator<String, Tuple1>(String.class, Tuple1.class)
        );

        this.operatorPlan.put(
                "clean",
                new FilterOperator<String>(
                        record -> {return false;},
                        String.class
                )
        );

        this.operatorPlan.put(
                "collect",
                new TextFileSink<String>(
                        this.outputFile,
                        record -> record.toString(),
                        String.class
                )
        );

        return new Operator[0];
    }

    @Override
    public RheemPlan generatePlan(){

        connect("source", 0, "identity", 0);
        if(this.sniffer) {
            connect("identity", 0, "grep", 0);
            connect("grep", 0, "clean", 0);
        }else {
            connect("identity", 0, "sniffer_1", 0);
            connect("sniffer_1", 0, "grep", 0);
            connect("grep", 0, "sniffer_2", 0);
            connect("sniffer_2", 0, "clean", 0);
        }
        connect("clean", 0, "collect", 0);

        return new RheemPlan(this.operatorPlan.get("collect"));
    }

    private void connect(String ope1, int out_port, String ope2, int in_port){
        Operator current_operator = this.operatorPlan.get(ope1);
        Operator next_operator = this.operatorPlan.get(ope2);
        current_operator.connectTo(out_port, next_operator, in_port);
    }
}
