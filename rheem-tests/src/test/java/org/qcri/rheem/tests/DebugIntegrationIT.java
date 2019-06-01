package org.qcri.rheem.tests;

import org.qcri.rheem.basic.collection.MultiplexCollection;
import org.qcri.rheem.basic.collection.MultiplexList;
import org.qcri.rheem.basic.collection.MultiplexStatus;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.debug.rheemplan.RheemPlanDebug;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;

import org.qcri.rheem.java.Java;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by bertty on 10-05-17.
 */
public class DebugIntegrationIT {





    public static void main(String ... args) throws InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException {
       // new Tuple2<>();
    //    new Tuple5<>();
    //    Class tmp = Class.forName("org.qcri.rheem.basic.data.Tuple2");
    //    tmp.newInstance();
    //    Thread.sleep(500);
       // new Tuple5<>();

        //dobleCollect();
        //triplesniffer();
   snifferAndMultiplex(); // */
      //  testMaps();
//        multiplexOperator();

       /* Tuple4<Integer, Double, String, Long> tuple = new Tuple4(new Integer(0), new Double(0), new String(""), new Long(0));


        for(int i = 0; i < Tuple4.class.getTypeParameters().length; i++){
            System.out.println(Tuple4.class.getTypeParameters()[i]);
        }


        System.out.println(tuple.getClass().getFields()[0].getGenericType().getClass());
        System.out.println(tuple.getClass().getMethod("getField1"));
       // System.out.println(((ParameterizedType)tuple.getClass().getFields()[0].getGenericType()).getRawType());
        System.out.println(((TypeVariableImpl)tuple.getClass().getTypeParameters()[0]).getBounds()[0]);
        System.out.println(((TypeVariableImpl)tuple.getClass().getTypeParameters()[0]).getBounds()[1]);
        System.out.println(((TypeVariableImpl)tuple.getClass().getTypeParameters()[0]).getBounds()[2]);
        System.out.println(((TypeVariableImpl)tuple.getClass().getTypeParameters()[0]).getBounds()[3]);
/*
        System.out.println( ((ParameterizedType)tuple.getClass().getGenericSuperclass()).getActualTypeArguments()[0] );

        List<Type> genericSupertypes = Stream.concat(
            Stream.concat(
                Arrays.<Field>stream(tuple.getClass().getFields()).map(
                    (Field element) -> {
                       return element.getGenericType();
                    }
                ),
                Stream.of(tuple.getClass().getGenericSuperclass())
            ),
            Stream.of(tuple.getClass().getGenericInterfaces())
        ).collect(Collectors.toList());

        for(Type lala: genericSupertypes){
            if(lala instanceof ParameterizedType) {
                System.out.println("la leche");
            }
            if(lala instanceof TypeVariableImpl){
                System.out.println("aqui llegue");
                TypeVariableImpl lili = (TypeVariableImpl) lala;

                System.out.println(lili);
            }
            System.out.println( ((ParameterizedType)lala).getRawType() );
            System.out.println(lala.getClass());
        }
   //     for(Map.Entry<String, Type> tmp: ReflectionUtils.getTypeParameters()){

     //   }
        System.out.println(tuple.getClass());



     //   System.out.println(Tuple.getTupleClass(2));
     //   System.out.println(Tuple.getTupleClass(4));
       // dobleCollect();
        //doblePlan();
      //  repeatTest(40000);
     //   loopPlan(5000);
       // loopJava(100000);
      //  loopTest(10000);
     /*   SparkContext sc = new SparkContext();





        JavaRDD<String> textFile = sc.textFile("hdfs://...");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("hdfs://...");*/

    }

    public static void mainwww(String ... args) throws IOException, URISyntaxException {

        // Instantiate Rheem and activate the Java backend.

        URI uri1 = new URI("file:///Users/bertty/Qatar/example/wordcount1M.txt");
        URI uri2 = new URI("file:///Users/bertty/Qatar/example/wordcount1M.ord2");
        URI uri3 = new URI("file:///Users/bertty/Qatar/example/wordcount1M.ord");
        URI uri4 = new URI("file:///Users/bertty/Qatar/example/wordcount10M.txt");
        URI uri5 = new URI("file:///Users/bertty/Qatar/example/wordcount100M.txt");

       // URI uri = new URI("file:///Users/bertty/Qatar/example/words.tql");
      //  Path path = Paths.get(uri);
//CHANGE SnifferOperator SnifferOperator
    //    onlyStream(uri);

        //graf(uri4);
     //    mainTwoStage(uri4);
     //    ordernar(uri, uri2);



    }
/*
    public static void graf(URI uri){

        TextFileSource textFileSource= new TextFileSource(uri.toString());
        RheemPlan rheemPlan = new RheemPlan(twoStageX(textFileSource,0));
        //    RheemPlan rheemPlan = twoStageX(textFileSource, 4);

        RheemContext rheemContext = new RheemContext()
        //        .with(Java.basicPlugin());
                  .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);


    //    DebugIntegrationIT.readerInstruction(rheemContext, textFileSource);

    }

    public static void graf2(URI uri){
        TextFileSource textFileSource= new TextFileSource(uri.toString());
        RheemPlan rheemPlan = new RheemPlanDebug(twoStageX(textFileSource,0));
        //    RheemPlan rheemPlan = twoStageX(textFileSource, 4);

        RheemContext rheemContext = new RheemContext()
                .with(Java.basicPlugin());
        //          .with(Spark.basicPlugin());
        rheemContext.execute(rheemPlan);


        /* ModeRun.stopProcess();

        RheemPlan rd = new RheemPlanDebug(twoStageX(textFileSource, 0));
        ModeRun.continueProcess();

        rheemContext.execute(rd);*
    }
*/

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
           //         ModeRun.stopProcess();
                    MapOperator map = new MapOperator(a -> {return "hola" + a;}, String.class, String.class);
                    List<String> words2 = new ArrayList<>();
                    textFileSource.connectTo(0, map, 0);
                    LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(words2, String.class );
                    map.connectTo(0, sink2, 0);

                    Thread.sleep(3000);
                    RheemPlan rheemPlan2 = (RheemPlan) new RheemPlanDebug(sink2);

       //             ModeRun.continueProcess();
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
/*
    public static void mainTwoStage(URI uri){

        TextFileSource textFileSource= new TextFileSource(uri.toString());
        RheemPlan rheemPlan = new RheemPlanDebug(twoStageX(textFileSource,0));
    //    RheemPlan rheemPlan = twoStageX(textFileSource, 4);

        RheemContext rheemContext = new RheemContext()
                .with(Java.basicPlugin());
      //          .with(Spark.basicPlugin());
        Thread monitor = new Thread(
            () ->{
                rheemContext.execute(rheemPlan);
            }
        );

        monitor.start();
        DebugIntegrationIT.readerInstruction(rheemContext, textFileSource);

    }
/*
    public static RheemPlan twoStage(Operator textFileSource){

 //       textFileSource.addTargetPlatform(Spark.platform());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
            new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                String.class,
                String.class
            )
        );
      /*  FilterOperator<String> windows = null;
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
        SnifferOperator<String> windows = new SnifferOperator<String>(String.class);
        SnifferOperator<String> windows2 = new SnifferOperator<String>(String.class);


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
      //  windows2.connectTo(0, flatMapOperator, 0);
       // flatMapOperator.connectTo(0, mapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
       // windows.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);
        return new RheemPlanDebug( sink);
    }

*/
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
     /*   try {
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
                        RheemPlan rd = new RheemPlanDebug(twoStageX(source, 3));
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
                        RheemPlan rd2 = new RheemPlanDebug(twoStageX(source, 4));
                        ModeRun.continueProcess();
                        Thread monitor2 = new Thread(
                                () ->{
                                    rheemContext.execute(rd2);
                                }
                        );
                        monitor2.start();
                        break;
                    case "kill":
                        SparkDebug.killSpark();
                        ModeRun.stopProcess();
                        break;
                    default:
                        instruction = null;
                        break;
                }


            } while (instruction != null);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
/*
    public static Operator twoStageX(Operator source, int filter){

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
        SnifferOperator<String> windows = new SnifferOperator<String>(String.class);

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

        SnifferOperator<Tuple2> windows2 = new SnifferOperator<Tuple2>(Tuple2.class);
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
    //    windows2.connectTo(0, sink, 0);

        return sink;
    }

*/
    public static void repeatTest(int numIterations){
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(1);
        CollectionSource<Integer> source = new CollectionSource<>(lista, Integer.class);

        RepeatOperator<Integer> repeat = new RepeatOperator<>(numIterations, Integer.class);

        repeat.setName("repeat");

        MapOperator<Integer, Integer> increment = new MapOperator<>(
                i -> i + 1, Integer.class, Integer.class
        );

        increment.setName("incrementar");
        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);

        sink.setName("sink pantalla");

        MapOperator<Integer, Integer> map2 = new MapOperator<>(
                i -> i , Integer.class, Integer.class
        );

        map2.setName("map the comprobation");


        repeat.initialize(source,0);
        repeat.beginIteration(increment,0);
        repeat.endIteration(increment,0);
        repeat.connectFinalOutputTo(map2,0);

        map2.connectTo(0, sink, 0);

        RheemPlan plan = new RheemPlan(sink);

        RheemContext rc = new RheemContext().with(Java.basicPlugin());

        rc.execute(plan);

    }


    public static void loopTest(int numIterations) {
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(1);
        CollectionSource<Integer> source = new CollectionSource<>(lista, Integer.class);

        ArrayList<Integer> lista1 = new ArrayList<>();
        lista1.add(0);
        CollectionSource<Integer> convergenceSource = new CollectionSource<>(lista1, Integer.class);

        LoopOperator<Integer, Integer> loopOperator = new LoopOperator<Integer, Integer>(DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) collection ->
                        collection.iterator().next() >= numIterations,
                numIterations
        );

        loopOperator.initialize(source, convergenceSource);

        MapOperator<Integer, Integer> counter = new MapOperator<>(
                new TransformationDescriptor<>(n -> n + 1, Integer.class, Integer.class)
        );


        MapOperator<Integer, Integer> counter2 = new MapOperator<>(
            new TransformationDescriptor<>(
                n -> {
                    System.out.println("pase CTM");
                    return n + 1;
                }, Integer.class, Integer.class)
        );

        FilterOperator<Integer> filter = new FilterOperator<Integer>(
                DataSetType.createDefault(Integer.class),
                i -> true
        );

        counter.connectTo(0, filter, 0);

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);


        loopOperator.beginIteration(counter, counter2);
        loopOperator.endIteration(filter, counter2);
        loopOperator.outputConnectTo(sink);

        RheemPlan plan = new RheemPlan(sink);

        RheemContext rc = new RheemContext().with(Java.basicPlugin());

        rc.execute(plan);
    }

    public static void loopPlan(int numIterations) {
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(0);
        CollectionSource<Integer> source = new CollectionSource<>(lista, Integer.class);


        MapOperator<Integer, Integer> previus = new MapOperator<>(
                i -> i + 1, Integer.class, Integer.class
        );

        source.connectTo(0, previus, 0);
        MapOperator<Integer, Integer> next = null;

        for(int j = 0; j < numIterations-1; j++){
            next = new MapOperator<>(
                    i -> i + 1, Integer.class, Integer.class
            );
            previus.connectTo(0, next, 0);
            previus = next;
        }

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);

        next.connectTo(0, sink, 0);

        RheemPlan plan = new RheemPlan(sink);

        RheemContext rc = new RheemContext().with(Java.basicPlugin());

        rc.execute(plan);

    }

    public static void loopJava(int numIterations){
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(0);

        Stream<Integer> stream = lista.stream();

        for(int j = 0; j < numIterations; j++){
            stream = stream.map(i -> i +1);
        }

        stream.forEach(System.out::println);

    }

    public static void doblePlan(){
        Collection<Integer> cole = Arrays.asList(1, 2, 3, 4, 5, 6, 7 );
        CollectionSource<Integer> source = new CollectionSource(cole, Integer.class);

        MapOperator<Integer, Integer> mapDerecha = new MapOperator<Integer, Integer>(
                element -> {
                    System.out.println("map Derecha: "+new Date().getTime());
                   return element * element;
                },
                Integer.class,
                Integer.class
        );

        MapOperator<Integer, Integer> mapIzquierda = new MapOperator<Integer, Integer>(
                element-> {
                    System.out.println("map Izquierda: "+new Date().getTime());
                    return element + 5;
                },
                Integer.class,
                Integer.class
        );

        LocalCallbackSink<Integer> sinkDerecha = LocalCallbackSink.createStdoutSink(Integer.class);
        LocalCallbackSink<Integer> sinkIzquierda = LocalCallbackSink.createStdoutSink(Integer.class);

        source.connectTo(0, mapDerecha, 0);
        source.connectTo(0, mapIzquierda, 0);
        mapDerecha.connectTo(0, sinkDerecha, 0);
        mapIzquierda.connectTo(0, sinkIzquierda, 0);

        RheemContext context = new RheemContext();
        context.register(Java.basicPlugin());


        RheemPlan plan = new RheemPlan(sinkDerecha, sinkIzquierda);

        context.execute(plan);
    }


    public static void dobleCollect(){
        MultiplexCollection<Integer> cole = new MultiplexList<>();
        System.out.println(cole);

        Thread hilo = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println(
                    "Stream2: "+
                            cole
                                .stream()
                                .filter(
                                    ele ->{
                                    System.out.println("ele: "+ele);
                                    if(ele > 6){
                                        try {
                                            Thread.sleep(2000);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                     return true;
                                })
                                .count()
                );
            }
        });
        hilo.setName("stream2");
        hilo.start();
        try {
            Thread.sleep(2000);


            for(int i = 0; i < 13; i++){
                cole.add(i);
                Thread.sleep(500);
                if(i == 4){
                    Thread hilo2 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            System.out.println(
                                    "Stream1: "+
                                            cole
                                                    .stream()
                                                    .filter(
                                                            ele ->{
                                                                System.out.println("ele2 : "+ele);
                                                                if(ele > 6){
                                                                    try {
                                                                        Thread.sleep(2000);
                                                                    } catch (InterruptedException e) {
                                                                        e.printStackTrace();
                                                                    }
                                                                }
                                                                return true;
                                                            })
                                                    .count()
                            );
                        }
                    });
                    hilo2.setName("stream1 ");
                    hilo2.start();
                }
                if(i == 6){
                    cole.setStatus(MultiplexStatus.KILLED);
                }
            }

            System.out.println(cole);

            cole.setStatus(MultiplexStatus.FINISH_INPUT);

            System.out.println(cole);

            System.out.println(cole.size());

            System.out.println(
                    Thread.activeCount()
            );

            int active = Thread.activeCount();
            System.out.println("currently active threads: " + active);
            Thread all[] = new Thread[active];
            Thread.enumerate(all);

            for (int i = 0; i < active; i++) {
                System.out.println(i + ": " + all[i]);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }



/*
        Iterator<Integer> iter1 = array.iterator();
        Iterator<Integer> iter2 = array.iterator();


        System.out.println("iter1 : "+ iter1.next());
        System.out.println("iter1 : "+ iter1.next());
        System.out.println("iter1 : "+ iter1.next());
        System.out.println("iter2 : "+ iter2.next());
        System.out.println("iter2 : "+ iter2.next());
        System.out.println("iter1 : "+ iter1.next());

        Stream<Integer> stream1 = array.stream().map(
                element -> {
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Stream 1: "+element);
                    return element;
                }
        );
        Stream<Integer> stream2 = array.stream().map(
                element -> {
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Stream 2: "+element);
                    return element;
                }
        );

        Thread hilo = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Stream2: "+ stream2.count() );
            }
        });

        hilo.start();

        System.out.println("Stream1: "+ stream1.count() );*/
    }

/*
    public static void multiplexOperator(){
        Collection<Integer> sources = new ArrayList<>();
        for(int  i = 0; i < 100; i++){
            sources.add(i);
        }

        CollectionSource<Integer> sourceOperator = new CollectionSource<Integer>(sources, Integer.class);
        sourceOperator.setName("source operator");

        FilterOperator<Integer> filterOperator = new FilterOperator<Integer>(
                element ->  {
                    try{
                        Thread.sleep(200    );
                    }catch(Exception e){}
                    return true;
                },
                Integer.class
        );
        filterOperator.setName("filter operator");

        SnifferOperator<Integer> snifferOperator = new SnifferOperator<Integer>(Integer.class);
        snifferOperator.setName("sniffer operator");


        MapOperator<Integer, Integer> mapOperator = new MapOperator<Integer, Integer>(
                a -> {return a;},
                Integer.class,
                Integer.class
        );
        mapOperator.setName("map operator");

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        sink.setName("real sink operator");

        TextFileSink<Integer> plagio = new TextFileSink<Integer>("file:///Users/bertty/sniffer.txt", Integer.class);
        plagio.setName("plagio sink operator");

        snifferOperator.connectTo(1, plagio, 0);

        sourceOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, snifferOperator, 0);
        snifferOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, sink, 0);

        RheemContext rc = new RheemContext();
        rc.register(Java.basicPlugin());

        RheemPlan rp = new RheemPlanDebug(sink, plagio);

        rc.execute(rp);
        int active = Thread.activeCount();
        System.out.println("currently active threads: " + active);
        Thread all[] = new Thread[active];
        Thread.enumerate(all);

        for (int i = 0; i < active; i++) {
            System.out.println(i + ": " + all[i]);
        }
    }
/*
    public static void triplesniffer(){
            Collection<Integer> sources = new ArrayList<>();
            for(int  i = 0; i < 100; i++){
                sources.add(i);
            }

            CollectionSource<Integer> sourceOperator = new CollectionSource<Integer>(sources, Integer.class);
            sourceOperator.setName("source operator");

            FilterOperator<Integer> filterOperator = new FilterOperator<Integer>(
                    element ->  {
                        try{
                            Thread.sleep(200    );
                        }catch(Exception e){}
                        return true;
                    },
                    Integer.class
            );
            filterOperator.setName("filter operator");

            SnifferOperator<Integer> snifferOperator = new SnifferOperator<Integer>(Integer.class);
            snifferOperator.setName("sniffer operator");


            MapOperator<Integer, Integer> mapOperator = new MapOperator<Integer, Integer>(
                    a -> {return a;},
                    Integer.class,
                    Integer.class
            );
            mapOperator.setName("map operator");

            FilterOperator<Integer> filterOperator2 = new FilterOperator<Integer>(
                    element ->  {
                        try{
                            Thread.sleep(200    );
                        }catch(Exception e){}
                        return element%2 == 0;
                    },
                    Integer.class
            );
            filterOperator2.setName("filter2 operator");


            FilterOperator<Integer> filterOperator3 = new FilterOperator<Integer>(
                    element ->  {
                        try{
                            Thread.sleep(200    );
                        }catch(Exception e){}
                        return element%2 == 1;
                    },
                    Integer.class
            );
            filterOperator3.setName("filter3 operator");

            SnifferOperator<Integer> snifferOperator2 = new SnifferOperator<Integer>(Integer.class);
            snifferOperator2.setName("sniffer2 operator");
            SnifferOperator<Integer> snifferOperator3 = new SnifferOperator<Integer>(Integer.class);
            snifferOperator3.setName("sniffer3 operator");

            LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
            sink.setName("real2 sink operator");
            LocalCallbackSink<Integer> sink2 = LocalCallbackSink.createStdoutSink(Integer.class);
            sink2.setName("real3 sink operator");


            TextFileSink<Integer> plagio = new TextFileSink<Integer>("file:///Users/bertty/sniffer.txt", Integer.class);
            plagio.setName("plagio sink operator");
            TextFileSink<Integer> plagio2 = new TextFileSink<Integer>("file:///Users/bertty/sniffer2.txt", Integer.class);
            plagio2.setName("plagio sink2 operator");
            TextFileSink<Integer> plagio3 = new TextFileSink<Integer>("file:///Users/bertty/sniffer3.txt", Integer.class);
            plagio3.setName("plagio sink3 operator");

            snifferOperator.connectTo(1, plagio, 0);
            snifferOperator2.connectTo(1, plagio2,0);
            snifferOperator3.connectTo(1, plagio3,0);

            sourceOperator.connectTo(0, filterOperator, 0);
            filterOperator.connectTo(0, snifferOperator, 0);
            snifferOperator.connectTo(0, mapOperator, 0);
            mapOperator.connectTo(0, filterOperator2, 0);
            mapOperator.connectTo(0, filterOperator3, 0);
            filterOperator2.connectTo(0, snifferOperator2, 0);
            filterOperator3.connectTo(0, snifferOperator3, 0);
            snifferOperator2.connectTo(0, sink, 0);
            snifferOperator3.connectTo(0, sink2, 0);




            RheemContext rc = new RheemContext().changeToDebug();
            rc.register(Java.basicPlugin());

            RheemPlan rp = new RheemPlanDebug(plagio, plagio2, plagio3, sink, sink2);

            rc.execute(rp);
            int active = Thread.activeCount();
            System.out.println("currently active threads: " + active);
            Thread all[] = new Thread[active];
            Thread.enumerate(all);

            for (int i = 0; i < active; i++) {
                System.out.println(i + ": " + all[i]);
            }
    }

    public static void testMaps(){
        Map<String, Integer> map;


        HashMap<String, Integer> hashMap = new HashMap<>();

        map = hashMap;


        map.put("one", 1);
        map.put("two", 2);
        map.put("one", 3);
        map.put("three", 4);
        map.put("one", 5);
        map.put("two", 6);

        System.out.println(map.get("one"));


        Hashtable<String, Integer> hashTable = new Hashtable<>();

        map = hashTable;


        map.put("one", 1);
        map.put("two", 2);
        map.put("one", 3);
        map.put("three", 4);
        map.put("one", 5);
        map.put("two", 6);

        System.out.println(map.get("one"));

        MultiMap<String, Integer> multiMap = new MultiMap<>();



        multiMap.putSingle("one", 1);
        multiMap.putSingle("one", 1);
        multiMap.putSingle("one", 1);
        multiMap.putSingle("one", 1);
        multiMap.putSingle("two", 2);
        multiMap.putSingle("one", 3);
        multiMap.putSingle("three", 4);
        multiMap.putSingle("one", 5);
        multiMap.putSingle("two", 6);

        System.out.println(multiMap.get("one"));
    }
*/
    public static void snifferAndMultiplex(){
        Collection<Integer> sources = new ArrayList<>();
        for(int  i = 0; i < 100; i++){
            sources.add(i);
        }

        CollectionSource<Integer> sourceOperator = new CollectionSource<Integer>(sources, Integer.class);
        sourceOperator.setName("source operator");

        FilterOperator<Integer> filterOperator = new FilterOperator<Integer>(
                element ->  {
                    try{
                        Thread.sleep(200    );
                    }catch(Exception e){}
                    return true;
                },
                Integer.class
        );
        filterOperator.setName("filter operator");

        ///########################################################################################################
        ///########################################################################################################
        ///################################   MULTIPLEX                     #######################################
        ///########################################################################################################
        ///########################################################################################################

        SnifferOperator<Integer, Tuple1> snifferOperator = new SnifferOperator<Integer, Tuple1>(Integer.class, Tuple1.class);
        snifferOperator.setName("sniffer operator");
        snifferOperator.setFunction(
                element -> {
                    return new Tuple1<Integer>(element);
                }
        );

        TupleType tupleType = new TupleType<Tuple2>(Tuple3.class, String.class, Integer.class, Date.class);
        MultiplexOperator multiplexOperator = new MultiplexOperator(
                        DataSetType.createDefault(BasicDataUnitType.createBasic(Tuple1.class)),
                        tupleType,
                false
                );
        multiplexOperator.setName("multiplex");

        FunctionDescriptor.SerializableFunction<Tuple1, String> fun0 = ele -> {return "string: "+ ele.getField(0);};
        FunctionDescriptor.SerializableFunction<Tuple1, Integer> fun1 =  ele -> {return (Integer) ele.getField(0);};
        FunctionDescriptor.SerializableFunction<Tuple1, Date> fun2 = ele -> {
            try{
                Thread.sleep(1000);
            }catch(Exception e){

            }
            return new Date(System.currentTimeMillis());
        };
        try {
            multiplexOperator.setFunction(0, fun0);
            multiplexOperator.setFunction(1, fun1);
            multiplexOperator.setFunction(2, fun2);

        }catch (Exception e){
            System.out.println(e);
            System.exit(0);
        }

        TextFileSink<String>  plagio0 = new TextFileSink("file:///Users/bertty/sniffer0.txt", String.class);
        TextFileSink<Integer> plagio1 = new TextFileSink("file:///Users/bertty/sniffer1.txt", Integer.class);
        TextFileSink<Date> plagio2 = new TextFileSink("file:///Users/bertty/sniffer2.txt", Date.class);
        plagio0.setName("plagio0 sink");
        plagio1.setName("plagio1 sink");
        plagio2.setName("plagio2 sink");


        multiplexOperator.connectTo(0, plagio0, 0);
        multiplexOperator.connectTo(1, plagio1, 0);
        multiplexOperator.connectTo(2, plagio2, 0);

        snifferOperator.connectTo(1, multiplexOperator, 0);

        ///########################################################################################################
        ///########################################################################################################
        ///########################################################################################################
        ///########################################################################################################
        ///########################################################################################################
        ///########################################################################################################
        ///########################################################################################################





        MapOperator<Integer, Integer> mapOperator = new MapOperator<Integer, Integer>(
                a -> {return a;},
                Integer.class,
                Integer.class
        );
        mapOperator.setName("map operator");

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        sink.setName("real sink operator");

        sourceOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, snifferOperator, 0);
        snifferOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, sink, 0);

        RheemContext rc = new RheemContext().changeToDebug();
        System.out.println(rc.getModeRun()+" : "+rc.getModeRun().isDebugMode());
        rc.register(Java.basicPlugin());

        RheemPlan rp = new RheemPlanDebug(sink, plagio0, plagio1, plagio2);

        rc.execute(rp);
        int active = Thread.activeCount();
        System.out.println("currently active threads: " + active);
        Thread all[] = new Thread[active];
        Thread.enumerate(all);

        for (int i = 0; i < active; i++) {
            System.out.println(i + ": " + all[i]);
        }

    }
}
/***
 *
 * passport
 * copia del grado y certificado
 * certificado del tipo de sangre
 * Certificado de antecedentes
 *
 *01 - Certificate Original
  02 - Certificate Translation
  03 - Clearance Original
  04 - Clearance Translation
  05 - Degree Original
  06 - Degree Translation
  07 - Academic transcript Original
  08 - Academic transcript Translation
  09 - Blood Type Original
 *
 *
 *
 */
