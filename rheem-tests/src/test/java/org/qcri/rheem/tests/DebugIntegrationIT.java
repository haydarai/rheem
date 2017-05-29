package org.qcri.rheem.tests;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.debug.rheemplan.RheemPlanDebug;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.Java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by bertty on 10-05-17.
 */
public class DebugIntegrationIT {


    public static void main(String ... args) throws IOException, URISyntaxException {

        // Instantiate Rheem and activate the Java backend.

        URI uri = new URI("file:///Users/bertty/Qatar/example/wordcount1M.txt");
        URI uri2 = new URI("file:///Users/bertty/Qatar/example/wordcount1M.ord2");
        URI uri3 = new URI("file:///Users/bertty/Qatar/example/wordcount1M.ord");
        URI uri4 = new URI("file:///Users/bertty/Qatar/example/wordcount10M.txt");

       // URI uri = new URI("file:///Users/bertty/Qatar/example/words.tql");
      //  Path path = Paths.get(uri);

    //    onlyStream(uri);
         mainTwoStage(uri4);
     //    ordernar(uri, uri2);
    }


    public static void onlyStream(URI uri, RheemContext rheemContext){
        List<String> words = new ArrayList<>();
        TextFileSource textFileSource = new TextFileSource(uri.toString());
        textFileSource.setName("fileinput");

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(words, String.class );
        textFileSource.connectTo(0, sink, 0);

        RheemPlan rheemPlan = (RheemPlan) new RheemPlanDebug(sink);


        Thread nueva = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    ModeRun.stopProcess();
                    MapOperator map = new MapOperator(a -> {return "hola" + a;}, String.class, String.class);
                    List<String> words2 = new ArrayList<>();
                    textFileSource.connectTo(0, map, 0);
                    LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(words2, String.class );
                    map.connectTo(0, sink2, 0);

                    Thread.sleep(3000);
                    RheemPlan rheemPlan2 = (RheemPlan) new RheemPlanDebug(sink2);

                    ModeRun.continueProcess();
                    rheemContext.execute(rheemPlan2);


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        nueva.start();
        rheemContext.execute(rheemPlan);
        System.out.println(words.get(0));

        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");
        System.out.println("#############################");

    }


    public static RheemPlan newPlan(Operator... op){
        MapOperator map = new MapOperator(a -> {return "hola" + a;}, String.class, String.class);

        LocalCallbackSink sink = LocalCallbackSink.createStdoutSink( String.class );

        op[0].connectTo(0, map, 0);

        map.connectTo(0, sink, 0);

        return  (RheemPlan) new RheemPlanDebug(sink);

    }

    public static void mainTwoStage(URI uri){



        TextFileSource textFileSource= new TextFileSource(uri.toString());
        RheemPlan rheemPlan = twoStage(textFileSource);

        RheemContext rheemContext = new RheemContext()
                .with(Java.basicPlugin());

       Thread monitor = new Thread(
            () ->{
                rheemContext.execute(rheemPlan);
            }
        );

        monitor.start();
        DebugIntegrationIT.readerInstruction(rheemContext, textFileSource);

    }

    public static RheemPlan twoStage(Operator textFileSource){

 //       textFileSource.addTargetPlatform(Spark.platform());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
            new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                String.class,
                String.class
            )
        );
    /*    URI uri = null;
        TextFileSink<String> windows = null;
        try {
            uri = new URI("file:///Users/bertty/Qatar/example/windows.txt");
            windows = new TextFileSink<String>(uri.toString(), String.class);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } */
   //     flatMapOperator.addTargetPlatform(Spark.platform());
        FilterOperator<String> windows = null;
        try {

            Socket s = new Socket("localhost", 9090);
            PrintWriter out = new PrintWriter(s.getOutputStream());

            windows = new FilterOperator<String>(
                    word -> {
                        out.println(word);
                        return true;
                    },
                    String.class
            );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
            new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                DataUnitType.createBasic(String.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
            ), DataSetType.createDefault(String.class),
            DataSetType.createDefaultUnchecked(Tuple2.class)
        );
  //      mapOperator.addTargetPlatform(Spark.platform());


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
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
  //      reduceByOperator.addTargetPlatform(Spark.platform());


        // write results to a sink

        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createStdoutSink(DataSetType.createDefault(Tuple2.class));
       //s LocalCallbackSink<String> windows = LocalCallbackSink.createWindows(String.class);
  //      sink.addTargetPlatform(Java.platform());

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, windows, 0);
        windows.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new RheemPlanDebug( sink);
    }


    public static void ordernar(URI uri, URI uri2){

        TextFileSource text = new TextFileSource(uri.toString());

        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );

        SortOperator<String, String> sort = new SortOperator<String, String>(
                s -> s,
                String.class,
                String.class
        );

        TextFileSink<String> sink = new TextFileSink<String>(uri2.toString(), String.class);

        text.connectTo(0, sort, 0);
      //  flatMapOperator.connectTo(0, sort, 0);
        sort.connectTo(0, sink,0);

        RheemContext rc = new RheemContext().with(Java.basicPlugin());

        RheemPlan plan = new RheemPlan(sink);

        rc.execute(plan);

    }

    public static void readerInstruction(RheemContext rheemContext, Operator source){
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            String instruction;
            do {
                instruction = br.readLine();
                switch (instruction){
                    case "stop":
                        ModeRun.stopProcess();
                        break;
                    case "pause":
                        ModeRun.pauseProcess();
                        break;
                    case "continue":
                        ModeRun.continueProcess();
                        break;
                    case "change":
                        ModeRun.stopProcess();
                        RheemPlanDebug rd = twoStageX(source, 3);
                        ModeRun.continueProcess();
                        Thread monitor = new Thread(
                                () ->{
                                    rheemContext.execute(rd);
                                }
                        );
                        monitor.start();
                        break;
                    case "change5":
                        ModeRun.stopProcess();
                        RheemPlanDebug rd2 = twoStageX(source, 4);
                        ModeRun.continueProcess();
                        Thread monitor2 = new Thread(
                                () ->{
                                    rheemContext.execute(rd2);
                                }
                        );
                        monitor2.start();
                    default:
                        instruction = null;
                        break;
                }


            } while (instruction != null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static RheemPlanDebug twoStageX(Operator source, int filter){

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );

        FilterOperator<String> filterOperator = new FilterOperator<String>(
                a -> a.length() > filter,
                String.class
        );
        FilterOperator<String> windows = null;
        try {

            Socket s = new Socket("localhost", 9090);
            PrintWriter out = new PrintWriter(s.getOutputStream());

            windows = new FilterOperator<String>(
                    word -> {
                        out.println(word);
                        return true;
                    },
                    String.class
            );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        //      mapOperator.addTargetPlatform(Spark.platform());


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
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
        //      reduceByOperator.addTargetPlatform(Spark.platform());


        // write results to a sink

        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createStdoutSink(DataSetType.createDefault(Tuple2.class));
     //   LocalCallbackSink<String> windows = LocalCallbackSink.createWindows(String.class);
        //      sink.addTargetPlatform(Java.platform());

        // Build Rheem plan by connecting operators
        source.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, windows, 0);
        windows.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);


        return new RheemPlanDebug( sink );
    }

}

