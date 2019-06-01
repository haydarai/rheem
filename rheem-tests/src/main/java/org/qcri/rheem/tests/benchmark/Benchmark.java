package org.qcri.rheem.tests.benchmark;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.plugin.JavaBasicPlugin;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public abstract class Benchmark {

    //protected String path = "file:///D:\\rheem-debug-mode\\rheem\\rheem-tests\\src\\test\\resources\\big_words.txt";
    protected String path = "file:///D:\\rheem-debug-mode\\rheem\\rheem-tests\\src\\test\\resources\\words_1g.txt";
    protected int size_filter = 5;

    protected Map<String, Operator> operators = new HashMap<>();

    public Benchmark(){
        buildBase();
    }

    protected RheemContext preExecute(){
        return new RheemContext().with(new JavaBasicPlugin());
    }

    protected abstract RheemPlan doExecute();

    protected void postExecute(RheemContext context, RheemPlan plan){
        context.execute(plan);
    }

    public void execute(){
        postExecute(preExecute(), doExecute());
    }

    private void buildBase(){

        this.operators.put("source",
            new TextFileSource(
                this.path
            )
        );

        this.operators.put("cut",
            new FlatMapOperator<String, String>(
                line -> Arrays.asList(line.trim().split(" ")),
                String.class,
                String.class
            )
        );

        this.operators.put("labels",
            new MapOperator<String, Tuple3>(
                new MapLabel(line -> { return line.length() < size_filter; } ),
                String.class,
                Tuple3.class
            )
        );

        this.operators.put("reduce",
            new ReduceByOperator<Tuple3, ConditionFilter>(
                tuple3 -> {
                    return (ConditionFilter) tuple3.field0;
                },
                (first, second) -> {
                    int sum = (int) first.field2 + (int) second.field2;
                    return new Tuple3(first.field0, null, sum);
                },
                ConditionFilter.class,
                Tuple3.class
            )
        );

        this.operators.put("sink",
            LocalCallbackSink.createStdoutSink(
                Tuple3.class
            )
        );

        this.operators.put("sink_empty",
            new EmptySink<Tuple2>(
                Tuple2.class
            )
        );

        this.operators.put("sniffer",
            new SnifferOperator<Tuple3, Tuple2>(Tuple3.class, Tuple2.class).setFunction(
                element -> {
                    return new Tuple2<ConditionFilter, String>((ConditionFilter)element.field0, (String)element.field1);
                }
            )
        );

    }

    protected void connect(String name0, String name1){
        connect(name0, 0, name1, 0);
    }
    protected void connect(String name0, int index_output, String name1, int index_input){
        connect(this.operators.get(name0), index_output, this.operators.get(name1), index_input);
    }

    protected void connect(Operator op0, int index_output, Operator op1, int index_input){
        op0.connectTo(index_output, op1, index_input);
    }


    protected static RheemContext getDebug(RheemContext original){
        RheemContext rc = new RheemContext().changeToDebug();
        rc.register(Java.basicPlugin());
        //DebugContext dc = rc.getDebugContext();
        return rc;
    }


    enum ConditionFilter{
        TRUE, FALSE
    }

    enum SeeTime{
        ALL, MEMORY, DISK, SNIFFER
    }



    static class MapLabel implements FunctionDescriptor.SerializableFunction<String, Tuple3>{

        Predicate<String> predicate;

        MapLabel(Predicate<String> predicate){
            this.predicate = predicate;
        }

        @Override
        public Tuple3 apply(String s) {
            ConditionFilter cond = (this.predicate.test(s))? ConditionFilter.TRUE: ConditionFilter.FALSE;
            return new Tuple3<ConditionFilter, String, Integer>(cond, s, 1);
        }
    }

    static class FlapMapCount implements FunctionDescriptor.ExtendedSerializableFunction<String, Iterable<String>>{

        public int count;
        private ModeRun mode;
        private int pause = -1;

        public void setModeRun(ModeRun mode){
            this.mode = mode;
        }

        public void setPause(int pause){
            this.pause = pause;
        }

        @Override
        public Iterable<String> apply(String s) {
            this.count++;
            if(this.count == this.pause){
                this.mode.stopProcess();
            }
            s = s.trim();
            return Arrays.asList(s.split(" "));
        }

        @Override
        public void open(ExecutionContext ctx) {
        }
    }
}
