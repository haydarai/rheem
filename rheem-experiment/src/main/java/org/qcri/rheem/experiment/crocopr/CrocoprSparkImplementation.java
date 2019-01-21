package org.qcri.rheem.experiment.crocopr;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.experiment.crocopr.udf.ParserDBPedia;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import java.util.Arrays;

final public class CrocoprSparkImplementation extends SparkImplementation {

    private static final float DAMPENING_FACTOR = 0.85f;
    private static final float EPSILON = 0.001f;

    public CrocoprSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String input2 = ((FileParameter) parameters.getParameter("input2")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        JavaRDD<Tuple2<String, String>> file1 = readInput(input);
        JavaRDD<Tuple2<String, String>> file2 = readInput(input2);


        JavaRDD<Tuple2<String, String>> all_links = file1.union(file2).distinct();


        JavaPairRDD<String, java.lang.Long> vertextid = all_links
                .flatMap(
                    tuple -> Arrays.asList(tuple._1(), tuple._2()).iterator()
                )
                .distinct()
                .zipWithUniqueId();

        JavaPairRDD<Long, Tuple2<String, Long>> vertextid_other_key = vertextid.keyBy(record -> record._2());

        ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);

        RDD<Edge<Object>> edges = all_links.keyBy(tuple -> tuple._2())
                .join(vertextid)
                .map(v1 ->  new Tuple2<Long, String>(
                                v1._2()._2(),
                                v1._2()._1()._2()
                        )
                )
                .keyBy(tuple -> tuple._2())
                .join(vertextid)
                .map(v1 -> {
                    return new Edge<Object>(
                            v1._2()._1()._1(),
                            v1._2()._2(),
                            objectTag
                    );
                }).rdd();


        ClassTag<Long> longClassTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);


        Graph<Object, Object> graph = Graph.fromEdges(edges, null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, objectTag);

        PageRank
                .run(graph, 10, 1d - DAMPENING_FACTOR, objectTag, objectTag)
                .vertices()
                .toJavaRDD()
                .map(
                    tuple -> {
                        return new Tuple2<Long, Float>(((Long) tuple._1()), ((Double) tuple._2()).floatValue());
                    }
                )
                .keyBy(tuple -> tuple._1())
                .join(vertextid_other_key)
                .map(tuple -> new Tuple<String, Float>(tuple._2()._2()._1(), tuple._2()._1()._2()))
                .saveAsTextFile(output);

    }

    private JavaRDD<Tuple2<String, String>> readInput(String input){
        return this.sparkContext
                .textFile(input)
                .filter(line -> !line.startsWith("#"))
                .map(new Function<String, Tuple2<String, String>>() {
                    ParserDBPedia parser = new ParserDBPedia();

                    @Override
                    public Tuple2<String, String> call(String v1) throws Exception {
                        String[] elements = parser.components(v1);
                        return new Tuple2<String, String>(elements[0], elements[1]);
                    }
                });

    }

}
