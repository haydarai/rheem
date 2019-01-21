package org.qcri.rheem.experiment.simwords;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.simword.SparseVector;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final public class SimWordsSparkImplementation extends SparkImplementation {
    public SimWordsSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
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

        Map<String, Object> parts = SimWordsSparkImplementation.firstPart(this.sparkContext, input, minWordOccurrences, neighborhoodReach);
        JavaRDD<Tuple2<Integer, String>> words_id = (JavaRDD<Tuple2<Integer, String>>) parts.get("words_id");
        JavaRDD<Tuple2<Integer, SparseVector>> wordVectors = (JavaRDD<Tuple2<Integer, SparseVector>>) parts.get("wordVectors");

        JavaRDD<Tuple2<Integer, SparseVector>> centroids = words_id
                .map(
                    tuple -> tuple._1
                )
                .groupBy(number -> number)
                .flatMap(
                    new FlatMapFunction<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, SparseVector>>() {
                        @Override
                        public Iterator<Tuple2<Integer, SparseVector>> call(Tuple2<Integer, Iterable<Integer>> tuple) throws Exception {
                            ArrayList<Tuple2<Integer, SparseVector>> tuples = new ArrayList<>();
                            int[] idArray = StreamSupport.stream(tuple._2().spliterator(), false)
                                    .mapToInt(i -> i)
                                    .toArray();

                            for(int i = 0; i < number_cluster; i++){
                                tuples.add(new Tuple2<Integer, SparseVector>(i, SparseVector.createRandom(idArray, 0.99d, number_cluster, false)));
                            }
                            return tuples.iterator();
                        }
                    }
                );

        List<Tuple2<Integer, SparseVector>> centroids_collection = centroids.collect();
        for(int i = 0; i < iterations; i++){
            Broadcast<List<Tuple2<Integer, SparseVector>>> broadcast_centroids = this.sparkContext.broadcast(centroids_collection);
            centroids_collection = wordVectors
                .map(
                    new SelectNearestCentroidSpark(broadcast_centroids)
                )
                .map(
                    tuple3 -> new Tuple2<Integer, SparseVector>(tuple3._3(), tuple3._2())
                )
                .keyBy(
                    tuple -> tuple._1()
                )
                .reduceByKey(
                    new Function2<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>>() {
                        @Override
                        public Tuple2<Integer, SparseVector> call(Tuple2<Integer, SparseVector> v1, Tuple2<Integer, SparseVector> v2) throws Exception {
                            return new Tuple2<Integer, SparseVector>(v1._1(), v1._2().$plus(v2._2()));
                        }
                    }
                )
                .map(tuple -> {
                    tuple._2()._2().normalize();
                    return tuple._2();
                })
                .collect();
        }

        Broadcast<List<Tuple2<Integer, SparseVector>>> final_centroids = this.sparkContext.broadcast(centroids_collection);
        Broadcast<List<Tuple2<Integer, String>>> broadcast_words_id = (Broadcast<List<Tuple2<Integer, String>>>) parts.get("broadcast_words_id");

        wordVectors
            .map(
                new SelectNearestCentroidSpark(final_centroids)
            )
            .map(
                tuple -> {
                    List<Integer> tmp= new ArrayList<>();
                    tmp.add(tuple._1());
                    return new Tuple2<Integer, List<Integer>>(tuple._3(), tmp);
                }
            )
            .keyBy(tuple -> tuple._1())
            .reduceByKey(
                new Function2<Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>, Tuple2<Integer, List<Integer>>>() {
                    @Override
                    public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, List<Integer>> v1, Tuple2<Integer, List<Integer>> v2) throws Exception {
                        v1._2().addAll(v2._2());
                        return v1;
                    }
                }
            )
            .map(tuple -> tuple._2()._2())
            .map(new ResolveClusterFunctionSpark(broadcast_words_id))
            .saveAsTextFile(output);
    }

    public static Map<String, Object> firstPart(JavaSparkContext sparkContext, String input, int minWordOccurrences, int neighborhoodReach){
        HashMap<String, Object> parts = new HashMap<>();

        JavaRDD<Tuple2<Integer, String>> words_id = sparkContext.textFile(input)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                        List<Tuple2<String, Integer>> collect = Arrays.stream(s.split("\\W+"))
                                .filter(word -> !word.isEmpty())
                                .map(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1))
                                .collect(Collectors.toList());
                        return collect.iterator();
                    }
                })
                .keyBy(tuple -> tuple._1())
                .reduceByKey(
                    new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                            return new Tuple2<>(v1._1(), v1._2() + v2._2());
                        }
                    }
                )
                .filter(tuple -> tuple._2()._2() > minWordOccurrences)
                .map(tuple -> tuple._1())
                .zipWithUniqueId()
                .map(tuple -> new Tuple2<Integer, String>(tuple._2().intValue(), tuple._1()) );


        Broadcast<List<Tuple2<Integer, String>>> broadcast_words_id = sparkContext.broadcast(words_id.collect());

        JavaRDD<Tuple2<Integer, SparseVector>> wordVectors = sparkContext.textFile(input)
            .flatMap(
                new CreateWordNeighborhoodFunctionSpark(broadcast_words_id, neighborhoodReach)
            )
            .keyBy(tuple -> tuple._1())
            .reduceByKey(
                new Function2<Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>, Tuple2<Integer, SparseVector>>() {
                    @Override
                    public Tuple2<Integer, SparseVector> call(Tuple2<Integer, SparseVector> v1, Tuple2<Integer, SparseVector> v2) throws Exception {
                        return new Tuple2<>(v1._1(), v1._2().$plus(v2._2()));
                    }
                }
            )
            .map(
                tuple -> {
                    tuple._2()._2().normalize();
                    return tuple._2();
                }
            );

        parts.put("words_id", words_id);
        parts.put("wordVectors", wordVectors);
        parts.put("broadcast_words_id", broadcast_words_id);
        return parts;
    }
}

class CreateWordNeighborhoodFunctionSpark implements FlatMapFunction<String, Tuple2<Integer, SparseVector>> {

    private boolean first = true;
    private Broadcast<List<Tuple2<Integer, String>>> broadcast_variable;
    private Map<String, Integer> dictionary;
    private ArrayList<String> collector = new ArrayList<>();
    private int neighborhoodReach;

    public CreateWordNeighborhoodFunctionSpark(Broadcast<List<Tuple2<Integer, String>>> broadcast_variable, int neighborhoodReach) {
        this.broadcast_variable = broadcast_variable;
        this.neighborhoodReach = neighborhoodReach;
    }

    @Override
    public Iterator<Tuple2<Integer, SparseVector>> call(String v1) throws Exception {
        if(first == true){
            open();
            first = false;
        }
        Arrays
            .stream( v1.split("\\W+") )
            .filter( word -> !word.isEmpty() )
            .forEach( collector::add );

        ArrayList<Tuple2<Integer, SparseVector>> out = new ArrayList<>();
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

                if (!builder.isEmpty()) out.add(new Tuple2<>(wordsIds.get(i), builder.build()));
            }
        }
        this.collector.clear();

        return out.iterator();
    }

    private void open(){
        this.dictionary = new HashMap<>();
        broadcast_variable.getValue().forEach(
                tuple -> dictionary.put(tuple._2(), tuple._1())
        );
    }
}

class SelectNearestCentroidSpark implements Function<Tuple2<Integer, SparseVector>, Tuple3<Integer, SparseVector, Integer>> {

    private boolean first = true;

    private List<Tuple2<Integer, SparseVector>> centroids;

    private Random random = new Random();
    private Broadcast<List<Tuple2<Integer, SparseVector>>> broadcast_centroids;

    public SelectNearestCentroidSpark(Broadcast<List<Tuple2<Integer, SparseVector>>> broadcast_centroids) {
        this.broadcast_centroids = broadcast_centroids;
    }

    public void open(){
        this.centroids = broadcast_centroids.getValue();
    }

    @Override
    public Tuple3<Integer, SparseVector, Integer> call(Tuple2<Integer, SparseVector> value) throws Exception {
        if(first == true){
            open();
            first = false;
        }
        double maxSimilarity = -1d;
        int nearestCentroid = -1;

        for(Tuple2<Integer, SparseVector> centroid : centroids){
            double similarity = Math.abs(centroid._2().$times(value._2()));
            if(similarity > maxSimilarity){
                maxSimilarity = similarity;
                nearestCentroid = centroid._1();
            }
        }
        if(nearestCentroid == -1){
            maxSimilarity = 0;
            nearestCentroid = this.centroids.get(this.random.nextInt(this.centroids.size()))._1();
        }

        return new Tuple3<>(value._1(), value._2(), nearestCentroid);
    }
}

class ResolveClusterFunctionSpark implements Function<List<Integer>, String> {

    private boolean first = true;

    private Map<Integer, String> map;
    private Broadcast<List<Tuple2<Integer, String>>> broadcast_words;

    public ResolveClusterFunctionSpark(Broadcast<List<Tuple2<Integer, String>>> broadcast_words) {
        this.broadcast_words = broadcast_words;
    }

    public void open(){
        this.map = new HashMap<>();
        broadcast_words.getValue().stream().forEach(
            tuple -> map.put(tuple._1(), tuple._2())
        );
    }

    @Override
    public String call(List<Integer> value) throws Exception {
        if(first == true) {
            open();
            first = false;
        }
        List<String> words = new ArrayList<>(value.size());
        value.forEach(
            element -> {
                words.add(map.getOrDefault(element, "???"));
            }
        );
        return words.toString();
    }
}



