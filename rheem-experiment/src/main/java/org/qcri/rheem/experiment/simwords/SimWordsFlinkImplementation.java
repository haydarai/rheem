package org.qcri.rheem.experiment.simwords;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.qcri.rheem.experiment.simword.SparseVector;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

final public class SimWordsFlinkImplementation extends FlinkImplementation {
    public SimWordsFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        int minWordOccurrences = ((VariableParameter<Integer>)parameters.getParameter("min")).getVariable();
        int neighborhoodReach = ((VariableParameter<Integer>)parameters.getParameter("neighborhoodReach")).getVariable();
        int number_cluster = ((VariableParameter<Integer>)parameters.getParameter("n_cluster")).getVariable();
        int iterations = ((VariableParameter<Integer>)parameters.getParameter("iterations")).getVariable();

        Map<String, DataSet> firstPart = firstPart(this.env, input, minWordOccurrences, neighborhoodReach);
        DataSet<Tuple2<Long, String>> words_id = firstPart.get("words_id");
        DataSet<Tuple2<Integer, SparseVector>> wordVectors = firstPart.get("wordVectors");

        ////TODO VALIDATE
        DataSet<Tuple2<Integer, SparseVector>> centroids = words_id
            .map(
                tuple -> {
                    return new Tuple2<Integer, Collection<Integer>>(tuple.f0.intValue(), new ArrayList<>(tuple.f0.intValue()));
            }).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Collection<Integer>>>(){}))
            .groupBy(0)

            .reduce(
                    new ReduceFunction<Tuple2<Integer, Collection<Integer>>>() {
                        @Override
                        public Tuple2<Integer, Collection<Integer>> reduce(Tuple2<Integer, Collection<Integer>> value1, Tuple2<Integer, Collection<Integer>> value2) throws Exception {
                            value1.f1.addAll(value2.f1);
                            return value1;
                        }
                    }
            )
            .flatMap(
                new FlatMapFunction<Tuple2<Integer, Collection<Integer>>, Tuple2<Integer, SparseVector>>() {


                    private List<Integer> elements;

                    @Override
                    public void flatMap(Tuple2<Integer, Collection<Integer>> value, Collector<Tuple2<Integer, SparseVector>> out) throws Exception {

                        int[] idArray = value.f1.stream().mapToInt(i -> i).toArray();
                        for (int i = 0; i < number_cluster; i++) {
                            out.collect(new Tuple2(i, SparseVector.createRandom(idArray, (double) 0.99, number_cluster, false)));
                        }
                    }
                }
        );

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Tuple2<Integer, SparseVector>> loop = centroids.iterate(iterations);


        DataSet<Tuple2<Integer, SparseVector>> newCentroids = wordVectors
                .map(new SelectNearestCentroidFlink("centroids")).withBroadcastSet(loop, "centroids")
                .map(
                    tuple3 -> new Tuple2<Integer, SparseVector>(tuple3.f2, tuple3.f1)
                ).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, SparseVector>>(){}))
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<Integer, SparseVector>>() {
                    @Override
                    public Tuple2<Integer, SparseVector> reduce(Tuple2<Integer, SparseVector> value1, Tuple2<Integer, SparseVector> value2) throws Exception {
                        value1.f1 = value1.f1.$plus(value2.f1);
                        return value1;
                    }
                })
                .map(
                    tuple -> {
                        tuple.f1.normalize();
                        return tuple;
                    }
                ).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, SparseVector>>(){}));

        // feed new centroids back into next iteration
        DataSet<Tuple2<Integer, SparseVector>> finalCentroids = loop.closeWith(newCentroids);

        wordVectors
                .map(
                    new SelectNearestCentroidFlink("finalCentroids")
                ).withBroadcastSet(finalCentroids, "finalCentroids")
                .map(tuple3 -> {
                    List<Integer> tmp= new ArrayList<>();
                    tmp.add(tuple3.f0);
                    return new Tuple2<Integer, List<Integer>>(
                        tuple3.f2,
                        tmp
                    );
                }).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, List<Integer>>>(){}))
                .groupBy(0)
                .reduce(
                    new ReduceFunction<Tuple2<Integer, List<Integer>>>() {
                        @Override
                        public Tuple2<Integer, List<Integer>> reduce(Tuple2<Integer, List<Integer>> value1, Tuple2<Integer, List<Integer>> value2) throws Exception {
                            value1.f1.addAll(value2.f1);
                            return value1;
                        }
                    }
                )
                .map(
                    new RichMapFunction<Tuple2<Integer, List<Integer>>, String>() {
                        private Map<Integer, String> map;
                        @Override
                        public void open(Configuration parameters) throws Exception {
                            this.map = new HashMap<>();
                            getRuntimeContext()
                                .<Tuple2<Long, String>>getBroadcastVariable("wordIds")
                                .forEach(
                                    tuple -> map.put(tuple.f0.intValue(), tuple.f1)
                                );
                        }

                        @Override
                        public String map(Tuple2<Integer, List<Integer>> value) throws Exception {
                            List<String> words = new ArrayList<>(value.f1.size());
                            value.f1.forEach(
                                element -> {
                                    words.add(map.getOrDefault(element, "???"));
                                }
                            );
                            return words.toString();
                        }
                    }
                ).withBroadcastSet(words_id,"wordIds")
                .writeAsText(output);
    }

    public static Map<String, DataSet> firstPart(ExecutionEnvironment env, String input, int minWordOccurrences, int neighborhoodReach){
        HashMap<String, DataSet> parts = new HashMap<>();

        // Create the word dictionary
        DataSet<String> forzipping = env.readTextFile(input)
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

        DataSet<Tuple2<Long, String>> words_id = DataSetUtils.zipWithUniqueId(forzipping);

        // Create the word neighborhood vectors.
        DataSet<Tuple2<Integer, SparseVector>> wordVectors = env.readTextFile(input).flatMap(
                new RichFlatMapFunction<String, Tuple2<Integer, SparseVector>>() {


                    private Map<String, Integer> dictionary;
                    private ArrayList<String> collector = new ArrayList<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
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
                                for (int j = i + 1; j < Math.min(wordsIds.size(), i + neighborhoodReach + 1); j++) {
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
        ).withBroadcastSet(words_id, "wordIds")
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


        parts.put("words_id", words_id);
        parts.put("wordVectors", wordVectors);

        return parts;
    }

}


class SelectNearestCentroidFlink extends RichMapFunction<Tuple2<Integer, SparseVector>, Tuple3<Integer, SparseVector, Integer>> {

    private String name_broadcast;

    private List<Tuple2<Integer, SparseVector>> centroids;

    private Random random = new Random();


    public SelectNearestCentroidFlink(String name_broadcast) {
        this.name_broadcast = name_broadcast;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().<Tuple2<Integer, SparseVector>>getBroadcastVariable(this.name_broadcast);
    }

    @Override
    public Tuple3<Integer, SparseVector, Integer> map(Tuple2<Integer, SparseVector> value) throws Exception {

        double maxSimilarity = -1d;
        int nearestCentroid = -1;

        for(Tuple2<Integer, SparseVector> centroid : centroids){
            double similarity = Math.abs(centroid.f1.$times(value.f1));
            if(similarity > maxSimilarity){
                maxSimilarity = similarity;
                nearestCentroid = centroid.f0;
            }
        }
        if(nearestCentroid == -1){
            maxSimilarity = 0;
            nearestCentroid = this.centroids.get(this.random.nextInt(this.centroids.size())).f0;
        }

        return new Tuple3<>(value.f0, value.f1, nearestCentroid);
    }
}
