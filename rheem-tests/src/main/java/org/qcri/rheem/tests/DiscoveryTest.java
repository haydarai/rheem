package org.qcri.rheem.tests;

import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.debug.DebugContext;
import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.debug.repository.Repository;
import org.qcri.rheem.core.debug.repository.RepositoryCollection;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Predicate;

public class DiscoveryTest {

    private boolean debugMode;
    private String  pathFile;
    private int     filter_size;
    private int     pause_moment;
    private boolean execute_new;
    private int     new_filter_size;
    private SeeTime seeTime;

    public DiscoveryTest(boolean debugMode, String pathFile, int filter_size, int pause_moment, boolean execute_new, int new_filter_size, String seeTime_string) {
        this.debugMode = debugMode;
        this.pathFile = pathFile;//RheemPlans.createUri(pathFile).toString();
        this.filter_size = filter_size;
        this.pause_moment = pause_moment;
        this.execute_new = execute_new;
        this.new_filter_size = new_filter_size;
        if(seeTime_string.compareToIgnoreCase("ALL") == 0 ){
            this.seeTime = SeeTime.ALL;
        }
        if(seeTime_string.compareToIgnoreCase( "MEMORY") == 0){
            this.seeTime = SeeTime.MEMORY;
        }
        if(seeTime_string.compareToIgnoreCase( "DISK") == 0){
            this.seeTime = SeeTime.DISK;
        }
        if(seeTime_string.compareToIgnoreCase("SNIFFER") == 0){
            this.seeTime = SeeTime.SNIFFER;
        }
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

    public void specialWordCount(){
        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Source Operator (Read the files )            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        int size_filter = this.filter_size;

        TextFileSource source = new TextFileSource(this.pathFile);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Flap Operator  (cut the lines in words)      #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Iterable<String>> flapfunction = new FlapMapCount();

        FlatMapOperator<String, String> cut_words = new FlatMapOperator<String, String>(
                flapfunction,
                String.class,
                String.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Map Operator  (put Labels)                   #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Tuple3> mapfunction = new MapLabel(line -> { return line.length() < size_filter; } );

        MapOperator<String, Tuple3> labels = new MapOperator<String, Tuple3>(
                mapfunction,
                String.class,
                Tuple3.class
        );

        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################    Reduce Operator  (count the words that have the labes) #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        ReduceByOperator<Tuple3, ConditionFilter> reduce = new ReduceByOperator<Tuple3, ConditionFilter>(
                tuple3 -> {
                    return (ConditionFilter) tuple3.field0;
                },
                (first, second) -> {
                    int sum = (int) first.field2 + (int) second.field2;
                    return new Tuple3(first.field0, null, sum);
                },
                ConditionFilter.class,
                Tuple3.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Sink Operator  (show the results)            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        LocalCallbackSink sink = LocalCallbackSink.createStdoutSink(Tuple3.class);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              make the structure the plan                  #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        source.connectTo(0, cut_words, 0);

        cut_words.connectTo(0, labels, 0);

        labels.connectTo(0, reduce, 0);

        reduce.connectTo(0, sink, 0);



        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Execute the plan in Rheem                    #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        RheemContext rc = new RheemContext();
        rc.register(Java.basicPlugin());
        rc.register(Spark.basicPlugin());

        RheemPlan rp = new RheemPlan(sink);

        rc.execute(rp);

        System.out.println(((FlapMapCount)flapfunction).count);
    }

    public DebugContext specialWordCountWithSniffer(){
        RheemContext rc = new RheemContext().changeToDebug();
        rc.register(Java.basicPlugin());
        rc.register(Spark.basicPlugin());
        DebugContext dc = rc.getDebugContext();
        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Source Operator (Read the files )            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        int size_filter = this.filter_size;

        TextFileSource source = new TextFileSource(this.pathFile);
        source.setName("input_file");


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Flap Operator  (cut the lines in words)      #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Iterable<String>> flapfunction = new FlapMapCount();

        ((FlapMapCount)flapfunction).setModeRun(dc.getModeRun());
        ((FlapMapCount)flapfunction).setPause(this.pause_moment);

        FlatMapOperator<String, String> cut_words = new FlatMapOperator<String, String>(
                flapfunction,
                String.class,
                String.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Map Operator  (put Labels)                   #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Tuple3> mapfunction = new MapLabel(line -> { return line.length() < size_filter; } );

        MapOperator<String, Tuple3> labels = new MapOperator<String, Tuple3>(
                mapfunction,
                String.class,
                Tuple3.class
        );

        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################    Reduce Operator  (count the words that have the labes) #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        ReduceByOperator<Tuple3, ConditionFilter> reduce = new ReduceByOperator<Tuple3, ConditionFilter>(
                tuple3 -> {
                    return (ConditionFilter) tuple3.field0;
                },
                (first, second) -> {
                    int sum = (int) first.field2 + (int) second.field2;
                    return new Tuple3(first.field0, null, sum);
                },
                ConditionFilter.class,
                Tuple3.class
        );

        reduce.setName("partial_output");


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Sink Operator  (show the results)            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################

        ArrayList<Tuple3> result = new ArrayList<>();
        //LocalCallbackSink<Tuple3> sink = LocalCallbackSink.createCollectingSink(result, Tuple3.class);
        LocalCallbackSink<Tuple3> sink = LocalCallbackSink.createStdoutSink( Tuple3.class);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################     Sniffer for save in memory (show the results)         #########################
        //##########################                                                           #########################
        //##############################################################################################################


        SnifferOperator<Tuple3, Tuple2> snifferOperator = new SnifferOperator<Tuple3, Tuple2>(Tuple3.class, Tuple2.class);
        snifferOperator.setName("sniffer");
        snifferOperator.setFunction(
                element -> {
                    return new Tuple2<ConditionFilter, String>((ConditionFilter)element.field0, (String)element.field1);
                }
        );

        TupleType tupleType = new TupleType<Tuple2>(Tuple2.class, String.class, String.class);

        MultiplexOperator multiplex = new MultiplexOperator(
                DataSetType.createDefault(BasicDataUnitType.createBasic(Tuple2.class)),
                tupleType,
                false
        );

        multiplex.setName("multiplex");
        System.out.println();
        FunctionDescriptor.SerializableFunction<Tuple2, String> fun0 = ele -> {
            return (((ConditionFilter)ele.field0) == ConditionFilter.TRUE)? (String)ele.field1: null;
        };

        FunctionDescriptor.SerializableFunction<Tuple2, String> fun1 = ele -> {
            return (((ConditionFilter)ele.field0) == ConditionFilter.FALSE)? (String)ele.field1: null;
        };

        try {
            multiplex.setFunction(0, fun0);
            multiplex.setFunction(1, fun1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        FilterOperator<String> filterTrue = new FilterOperator<String>(
            element -> {
                return element != null;
            },
            String.class
        );

        FilterOperator<String> filterFalse = new FilterOperator<String>(
            element -> { return element != null; },
            String.class
        );


        ArrayList<String> memoryTrue  = new ArrayList<String>(180219661);
        ArrayList<String> memoryFalse = new ArrayList<String>(19448357);
        LocalCallbackSink<String> sinkTrue = LocalCallbackSink.createCollectingSink(memoryTrue, String.class);
        LocalCallbackSink<String> sinkFalse = LocalCallbackSink.createCollectingSink(memoryFalse, String.class);
        sinkFalse.setName("sinkFalse");
        sinkTrue.setName("sinkTrue");

        Repository<String> repo_true = new RepositoryCollection<String>(memoryTrue, String.class);
        Repository<String> repo_false = new RepositoryCollection<String>(memoryFalse, String.class);

        dc.addRepository("part_true", repo_true);
        dc.addRepository("part_false", repo_false);


        multiplex.connectTo(0, filterTrue,0);
        multiplex.connectTo(1, filterFalse,0);

        filterTrue.connectTo(0, sinkTrue,0);
        filterFalse.connectTo(0, sinkFalse,0);

        snifferOperator.connectTo(1, multiplex, 0);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              make the structure the plan                  #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        source.connectTo(0, cut_words, 0);

        cut_words.connectTo(0, labels, 0);

        labels.connectTo(0, snifferOperator, 0);

        snifferOperator.connectTo(0, reduce, 0);

        reduce.connectTo(0, sink, 0);



        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Execute the plan in Rheem                    #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################

        RheemPlan rp = new RheemPlan(sink, sinkTrue, sinkFalse);

        rc.execute(rp);

        //result.stream().forEach(System.out::println);
        System.out.println("previous numero: "+((FlapMapCount)flapfunction).count);
        return dc;
    }


    public void specialWordCountWithSnifferPrima(DebugContext dc, SeeTime seeTime) {
        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Source Operator (Read the files )            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        int size_filter = this.new_filter_size;

        Iterator<String> input_source;
        if (seeTime != SeeTime.MEMORY) {
            input_source = dc.getInput("input_file");
        } else {
            input_source = Collections.EMPTY_LIST.iterator();
        }

        IteratorSource<String, String> source = new IteratorSource<String, String>(
                input_source,
                a -> a,
                String.class,
                String.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Flap Operator  (cut the lines in words)      #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Iterable<String>> flapfunction = new FlapMapCount();

        FlatMapOperator<String, String> cut_words = new FlatMapOperator<String, String>(
                flapfunction,
                String.class,
                String.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Map Operator  (put Labels)                   #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Tuple3> mapfunction = new MapLabel(line -> {
            return line.length() < size_filter;
        });

        MapOperator<String, Tuple3> labels = new MapOperator<String, Tuple3>(
                mapfunction,
                String.class,
                Tuple3.class
        );

        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################    Reduce Operator  (count the words that have the labes) #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        ReduceByOperator<Tuple3, ConditionFilter> reduce = new ReduceByOperator<Tuple3, ConditionFilter>(
                tuple3 -> {
                    return (ConditionFilter) tuple3.field0;
                },
                (first, second) -> {
                    int sum = (int) first.field2 + (int) second.field2;
                    return new Tuple3(first.field0, null, sum);
                },
                ConditionFilter.class,
                Tuple3.class
        );
        reduce.setName("final_output");


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Sink Operator  (show the results)            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        LocalCallbackSink sink = LocalCallbackSink.createStdoutSink(Tuple3.class);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################     Sniffer for save in memory (show the results)         #########################
        //##########################                                                           #########################
        //##############################################################################################################
        SnifferOperator<Tuple3, Tuple2> snifferOperator = new SnifferOperator<Tuple3, Tuple2>(Tuple3.class, Tuple2.class);
        snifferOperator.setName("sniffer");
        snifferOperator.setFunction(
                element -> {
                    return new Tuple2<ConditionFilter, String>((ConditionFilter) element.field0, (String) element.field1);
                }
        );

        TupleType tupleType = new TupleType<Tuple2>(Tuple2.class, String.class, String.class);

        MultiplexOperator multiplex = new MultiplexOperator(
                DataSetType.createDefault(BasicDataUnitType.createBasic(Tuple2.class)),
                tupleType,
                false
        );

        multiplex.setName("multiplex");

        FunctionDescriptor.SerializableFunction<Tuple2, String> fun0 = ele -> {
            return (((ConditionFilter) ele.field0) == ConditionFilter.TRUE) ? (String) ele.field1 : null;
        };

        FunctionDescriptor.SerializableFunction<Tuple2, String> fun1 = ele -> {
            return (((ConditionFilter) ele.field0) == ConditionFilter.FALSE) ? (String) ele.field1 : null;
        };

        try {
            multiplex.setFunction(0, fun0);
            multiplex.setFunction(1, fun1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        FilterOperator<String> filterTrue = new FilterOperator<String>(
                element -> {
                    return element != null;
                },
                String.class
        );

        FilterOperator<String> filterFalse = new FilterOperator<String>(
                element -> {
                    return element != null;
                },
                String.class
        );


        ArrayList<String> memoryTrue = new ArrayList<String>();
        ArrayList<String> memoryFalse = new ArrayList<String>();
        LocalCallbackSink<String> sinkTrue = LocalCallbackSink.createCollectingSink(memoryTrue, String.class);
        LocalCallbackSink<String> sinkFalse = LocalCallbackSink.createCollectingSink(memoryFalse, String.class);
        sinkFalse.setName("sinkFalse");
        sinkTrue.setName("sinkTrue");

        Repository<String> repo_true = new RepositoryCollection<String>(memoryTrue, String.class);
        Repository<String> repo_false = new RepositoryCollection<String>(memoryFalse, String.class);

        dc.addRepository("part_true_2", repo_true);
        dc.addRepository("part_false_2", repo_false);

        multiplex.connectTo(0, filterTrue, 0);
        multiplex.connectTo(1, filterFalse, 0);

        filterTrue.connectTo(0, sinkTrue, 0);
        filterFalse.connectTo(0, sinkFalse, 0);

        snifferOperator.connectTo(1, multiplex, 0);

        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Uso de la meta data                          #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################

        Collection<String> reprocesing_data;
        Collection<Tuple3> result_data;
        if (seeTime != SeeTime.DISK){
            reprocesing_data = ((RepositoryCollection) dc.getRepository("part_true")).getCollection();
            result_data = ((RepositoryCollection)dc.getRepository("partial_output")).getCollection();
        }else{
            reprocesing_data = Collections.EMPTY_LIST;
            result_data = Collections.EMPTY_LIST;
        }

        CollectionSource<String> reprocesing = new CollectionSource<String>(reprocesing_data, String.class);

        UnionAllOperator<String> unionSource = new UnionAllOperator<String>(String.class);

        reprocesing.connectTo(0, unionSource, 0);


        CollectionSource<Tuple3> result_previous = new CollectionSource<Tuple3>(result_data, Tuple3.class);

        UnionAllOperator<Tuple3> union_result = new UnionAllOperator<Tuple3>(Tuple3.class);

        FilterOperator<Tuple3> filter_result = new FilterOperator<Tuple3>(
                ele -> ((ConditionFilter)ele.field0) == ConditionFilter.FALSE,
                Tuple3.class
        );

        result_previous.connectTo(0, filter_result, 0);
        filter_result.connectTo(0, union_result, 0);




        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              make the structure the plan                  #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        source.connectTo(0, cut_words, 0);

        cut_words.connectTo(0, unionSource, 1);

        unionSource.connectTo(0, labels, 0);

        labels.connectTo(0, union_result, 1);

        union_result.connectTo(0, snifferOperator, 0);

        snifferOperator.connectTo(0, reduce, 0);

        reduce.connectTo(0, sink, 0);



        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Execute the plan in Rheem                    #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        RheemContext rc = new RheemContext().changeToDebug();
        rc.register(Java.basicPlugin());
        rc.register(Spark.basicPlugin());

        RheemPlan rp = new RheemPlan(sink, sinkTrue, sinkFalse);

        rc.execute(rp);

        System.out.println("Numero final: "+((FlapMapCount)flapfunction).count);
    }





    public DebugContext onlySniffer(){
        RheemContext rc = new RheemContext().changeToDebug();
        rc.register(Java.basicPlugin());
        rc.register(Spark.basicPlugin());
        DebugContext dc = rc.getDebugContext();
        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Source Operator (Read the files )            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        int size_filter = this.filter_size;

        TextFileSource source = new TextFileSource(this.pathFile);
        source.setName("input_file");


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Flap Operator  (cut the lines in words)      #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Iterable<String>> flapfunction = new FlapMapCount();

        ((FlapMapCount)flapfunction).setModeRun(dc.getModeRun());
        ((FlapMapCount)flapfunction).setPause(this.pause_moment);

        FlatMapOperator<String, String> cut_words = new FlatMapOperator<String, String>(
                flapfunction,
                String.class,
                String.class
        );


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Map Operator  (put Labels)                   #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        FunctionDescriptor.SerializableFunction<String, Tuple3> mapfunction = new MapLabel(line -> { return line.length() < size_filter; } );

        MapOperator<String, Tuple3> labels = new MapOperator<String, Tuple3>(
                mapfunction,
                String.class,
                Tuple3.class
        );

        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################    Reduce Operator  (count the words that have the labes) #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        ReduceByOperator<Tuple3, ConditionFilter> reduce = new ReduceByOperator<Tuple3, ConditionFilter>(
                tuple3 -> {
                    return (ConditionFilter) tuple3.field0;
                },
                (first, second) -> {
                    int sum = (int) first.field2 + (int) second.field2;
                    return new Tuple3(first.field0, null, sum);
                },
                ConditionFilter.class,
                Tuple3.class
        );

        reduce.setName("partial_output");


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Sink Operator  (show the results)            #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################

        ArrayList<Tuple3> result = new ArrayList<>();
        //LocalCallbackSink<Tuple3> sink = LocalCallbackSink.createCollectingSink(result, Tuple3.class);
        LocalCallbackSink<Tuple3> sink = LocalCallbackSink.createStdoutSink( Tuple3.class);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################     Sniffer for save in memory (show the results)         #########################
        //##########################                                                           #########################
        //##############################################################################################################


        SnifferOperator<Tuple3, Tuple1> snifferOperator = new SnifferOperator<Tuple3, Tuple1>(Tuple3.class, Tuple1.class);
        snifferOperator.setName("sniffer");
        snifferOperator.setFunction(
                element -> {
                    return new Tuple1<Tuple3>(element);
                }
        );

        TupleType tupleType = new TupleType<Tuple1>(Tuple1.class, Tuple3.class);

        MultiplexOperator multiplex = new MultiplexOperator(
                DataSetType.createDefault(BasicDataUnitType.createBasic(Tuple1.class)),
                tupleType,
                false
        );

        multiplex.setName("multiplex");

        FunctionDescriptor.SerializableFunction<Tuple1, Tuple3> fun0 = a -> {

            return (a == null)? null:  (Tuple3) a.field0;
        };


        try {
            multiplex.setFunction(0, fun0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        LocalCallbackSink<Tuple3> sinkDummy = new LocalCallbackSink<Tuple3>(
                a -> {
                    System.out.println("consumiendo");
                },
                Tuple3.class
        );

        sinkDummy.setName("test dummy");


        multiplex.connectTo(0, sinkDummy,0);


        snifferOperator.connectTo(1, multiplex, 0);


        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              make the structure the plan                  #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################
        source.connectTo(0, cut_words, 0);

        cut_words.connectTo(0, labels, 0);

        labels.connectTo(0, snifferOperator, 0);

        snifferOperator.connectTo(0, reduce, 0);

        reduce.connectTo(0, sink, 0);



        //##############################################################################################################
        //##############################################################################################################
        //##########################                                                           #########################
        //##########################              Execute the plan in Rheem                    #########################
        //##########################                                                           #########################
        //##############################################################################################################
        //##############################################################################################################

        RheemPlan rp = new RheemPlan(sink, sinkDummy);

        rc.execute(rp);

        //result.stream().forEach(System.out::println);
        System.out.println("previous numero: "+((FlapMapCount)flapfunction).count);
        return dc;
    }



    public void execute() throws InterruptedException {
        if( !debugMode ){
            specialWordCount();
            return;
        }

        if(this.seeTime == SeeTime.SNIFFER){
            onlySniffer();
            return;
        }

        DebugContext dc = specialWordCountWithSniffer();
        if(this.pause_moment != -1) {
            dc.getModeRun().continueProcess();
            Thread.sleep(10000);
            System.out.println("size_true: " + ((RepositoryCollection) dc.getRepository("part_true")).getCollection().size());
            System.out.println("size_false: " + ((RepositoryCollection) dc.getRepository("part_false")).getCollection().size());

            specialWordCountWithSnifferPrima(dc, this.seeTime);

        }

    }


    @Override
    public String toString() {
        return "debugMode: "+ this.debugMode +"\n"+
        "pathFile: "+ this.pathFile + "\n"+
        "filter_size: "+ this.filter_size + "\n"+
        "pause_moment: "+ this.pause_moment + "\n"+
        "execute_new: "+ this.execute_new + "\n"+
        "new_filter_size: "+ this.new_filter_size + "\n"+
        "seeTime: "+ this.seeTime + "\n";
    }

    public static void main(String... args) throws InterruptedException {
        DiscoveryTest discoveryTest = new DiscoveryTest(
                Boolean.parseBoolean(args[0]),
                args[1],
                Integer.parseInt(args[2]),
                Integer.parseInt(args[3]),
                Boolean.parseBoolean(args[4]),
                Integer.parseInt(args[5]),
                args[6]
        );
        System.out.println(discoveryTest);
        discoveryTest.execute();
    }





}
