package org.qcri.rheem.experiment.simwords;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.qcri.rheem.experiment.SparseVector;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.kmeans.Centroid;
import org.qcri.rheem.experiment.kmeans.KmeansFlinkImplementation;
import org.qcri.rheem.experiment.kmeans.Point;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final public class SimWordsFlinkImplementation extends FlinkImplementation {
    public SimWordsFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        /*String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        int minWordOccurrences = ((VariableParameter<Integer>)parameters.getParameter("min")).getVariable();
        int neighborhoodReach = ((VariableParameter<Integer>)parameters.getParameter("neighborhoodReach")).getVariable();
        int number_cluster = ((VariableParameter<Integer>)parameters.getParameter("n_cluster")).getVariable();
        int iterations = ((VariableParameter<Integer>)parameters.getParameter("iterations")).getVariable();

        // Create the word dictionary
        MapOperator<Tuple2<String, Integer>, String> forzipping = this.env.readTextFile(input)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(value.split("\\W+"))
                                .filter(word -> !word.isEmpty())
                                .map(word -> new Tuple2(word.toLowerCase(), 1))
                                .forEach(
                                        out::collect
                                );
                    }
                })
                .groupBy(1)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        value1.f1 += value2.f1;
                        return value1;
                    }
                })
                .filter(tuple -> tuple.f1 > minWordOccurrences)
                .map(tuple -> tuple.f0);

        DataSet<Tuple2<Long, String>> zipped = DataSetUtils.zipWithUniqueId(forzipping);

        // Create the word neighborhood vectors.
        MapOperator<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>> wordVectors = this.env.readTextFile(input).flatMap(
                new RichFlatMapFunction<String, Tuple2<Integer, SparseVector>>() {


                    private Map<String, Integer> dictionary;
                    private ArrayList<String> collector = new ArrayList<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        dictionary = new HashMap<>();
                        getRuntimeContext()
                                .<Tuple2<Long, String>>getBroadcastVariable("wordIds")
                                .stream()
                                .forEach(
                                        tuple -> dictionary.put(tuple.f1, tuple.f0.intValue())
                                );
                    }

                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, SparseVector>> out) throws Exception {
                        Arrays
                                .stream(
                                        value.split("\\W+")
                                )
                                .filter(word -> !word.isEmpty())
                                .forEach(
                                        collector::add
                                );

                        // Make sure that there is at least one neighbor; otherwise, the resulting vector will not support cosine similarity
                        if (this.collector.size() > 1) {

                            List<Integer> wordsIds = this.collector
                                    .stream()
                                    .map(
                                            word -> {
                                                return this.dictionary.getOrDefault(word, -1);
                                            }
                                    ).collect(Collectors.toList());

                            for (int i = 0; i < wordsIds.size(); i++) {
                                SparseVector.Builder builder = new SparseVector.Builder();
                                for (int j = Math.max(0, i - neighborhoodReach); j <= i; j++) {
                                    if (wordsIds.get(j) == -1) {
                                        continue;
                                    }
                                    builder.add(wordsIds.get(j), 1);
                                }
                                for (int j = i + 1; j <= Math.min(wordsIds.size(), i + neighborhoodReach + 1); j++) {
                                    if (wordsIds.get(j) == -1) {
                                        continue;
                                    }
                                    builder.add(wordsIds.get(j), 1);
                                }

                                if (!builder.isEmpty()) out.collect(new Tuple2<>(wordsIds.get(i), builder.build()));
                            }
                        }
                        this.collector.clear();
                    }

                }
            ).withBroadcastSet(zipped, "wordIds")
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple2<Integer, SparseVector>>() {
                @Override
                public Tuple2<Integer, SparseVector> reduce(Tuple2<Integer, SparseVector> value1, Tuple2<Integer, SparseVector> value2) throws Exception {
                    value1.f1 = value1.f1.$plus(value2.f1);
                    return value1;
                }
            })
            .map(new MapFunction<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>>() {
                @Override
                public Tuple2<Integer, SparseVector> map(Tuple2<Integer, SparseVector> value) throws Exception {
                     value.f1.normalize();
                     return value;
                }
            });


        FlatMapOperator<Object, Tuple2<Integer, SparseVector>> centroids = this.env.fromElements(null).flatMap(
                new RichFlatMapFunction<Object, Tuple2<Integer, SparseVector>>() {
                    private List<Integer> elements;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        elements = getRuntimeContext()
                                .<Tuple2<Long, String>>getBroadcastVariable("wordIds")
                                .stream()
                                .map(
                                        tuple -> tuple.f0.intValue()
                                ).collect(Collectors.toList());
                    }

                    @Override
                    public void flatMap(Object value, Collector<Tuple2<Integer, SparseVector>> out) throws Exception {
                        int[] idArray = elements.stream().mapToInt(i -> i).toArray();
                        for (int i = 0; i < number_cluster; i++) {
                            out.collect(new Tuple2(i, SparseVector.createRandom(idArray, (double) 0.99, number_cluster, false)));
                        }
                    }

                    ;
                }
        ).withBroadcastSet(zipped, "wordIds");


        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Tuple2<Integer, SparseVector>> loop = centroids.iterate(iterations);

        DataSet<Centroid> newCentroids = wordVectors
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new KmeansFlinkImplementation.CountAppender())
                .groupBy(0).reduce(new KmeansFlinkImplementation.CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new KmeansFlinkImplementation.CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new KmeansFlinkImplementation.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");




        // Apply the centroids to the points and resolve the word IDs.
        val clusters = wordVectors
                .mapJava(new SelectNearestCentroidFunction("finalCentroids")).withBroadcast(finalCentroids, "finalCentroids").withName("Select nearest final centroids")
      .map(assigment => (assigment._3, List(assigment._1))).withName("Discard word vectors")
                .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 ++ c2._2)).withName("Create clusters")
                .map(_._2).withName("Discard cluster IDs")
                .mapJava(new ResolveClusterFunction("wordIds")).withBroadcast(wordIds, "wordIds").withName("Resolve word IDs")
*/

    }
}
