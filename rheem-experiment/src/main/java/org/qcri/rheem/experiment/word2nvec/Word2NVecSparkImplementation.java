package org.qcri.rheem.experiment.word2nvec;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.simword.SparseVector;
import org.qcri.rheem.experiment.simwords.SimWordsSparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Tuple2;
import scala.Tuple3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final public class Word2NVecSparkImplementation extends SparkImplementation {
    public Word2NVecSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter)parameters.getParameter("input")).getPath();
        String output = ((FileResult)results.getContainerOfResult("output")).getPath();


        int minWordOccurrences = ((VariableParameter<Integer>)parameters.getParameter("min")).getVariable();
        int neighborhoodReach = ((VariableParameter<Integer>)parameters.getParameter("neighborhoodReach")).getVariable();

        Map<String, Object> parts = SimWordsSparkImplementation.firstPart(this.sparkContext, input, minWordOccurrences, neighborhoodReach);
        JavaRDD<Tuple2<Integer, String>> words_id = (JavaRDD<Tuple2<Integer, String>>) parts.get("words_id");
        JavaRDD<Tuple2<Integer, SparseVector>> wordVectors = (JavaRDD<Tuple2<Integer, SparseVector>>) parts.get("wordVectors");

        Broadcast<List<Tuple2<Integer, String>>> broadcast_words_id = (Broadcast<List<Tuple2<Integer, String>>>) parts.get("broadcast_words_id");

        wordVectors
            .map( new ExtendWordVector(broadcast_words_id) )
            .saveAsTextFile(output);

    }
}

class ExtendWordVector implements Function<Tuple2<Integer, SparseVector>, Tuple3<Integer, String, SparseVector>>{

    private boolean first = true;
    private Broadcast<List<Tuple2<Integer, String>>> broadcast_variable;
    private Map<Integer, String> map;

    public ExtendWordVector(Broadcast<List<Tuple2<Integer, String>>> broadcast_variable) {
        this.broadcast_variable = broadcast_variable;
    }

    @Override
    public Tuple3<Integer, String, SparseVector> call(Tuple2<Integer, SparseVector> value) throws Exception {
        if(first == true){
            open();
            first = false;
        }
        return new Tuple3<>(
                value._1(),
                map.getOrDefault(value._1(), "(unknown)"),
                value._2()
        );
    }


    private void open(){
        this.map = new HashMap<>();
        broadcast_variable.getValue().forEach(
            tuple -> map.put(tuple._1(), tuple._2())
        );
    }

}
