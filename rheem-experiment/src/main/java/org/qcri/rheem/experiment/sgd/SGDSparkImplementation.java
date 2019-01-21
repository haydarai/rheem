package org.qcri.rheem.experiment.sgd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

final public class SGDSparkImplementation extends SparkImplementation {
    public SGDSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        int features = ((VariableParameter<Integer>)parameters.getParameter("features")).getVariable();
        int sample_size = ((VariableParameter<Integer>)parameters.getParameter("sample_size")).getVariable();
        int max_iterations = ((VariableParameter<Integer>)parameters.getParameter("max_iterations")).getVariable();
        double accuracy = ((VariableParameter<Double>)parameters.getParameter("accuracy")).getVariable();

        double[] weights = new double[features];



        JavaRDD<double[]> transformBuilder = this.sparkContext
                .textFile(input)
                .mapPartitions(new TransformPerPartitionSpark(features));

        int iteration = 0;
        boolean validation = true;
        double[] new_weights = weights;
        do{
            weights = new_weights;
            Broadcast<double[]> broadcast_weight = this.sparkContext.broadcast(weights);

            double[] partial_result =
                    this.sparkContext
                        .parallelize(
                            transformBuilder
                                .takeSample(false, sample_size)
                        )
                        .mapPartitions( new ComputeLogisticGradientPerPartitionSpark(features, broadcast_weight))
                        .reduce(
                            (a, b) -> {
                                double[] g1 = a;
                                double[] g2 = b;

                                if (g2 == null) //samples came from one partition only
                                    return g1;

                                if (g1 == null) //samples came from one partition only
                                    return g2;

                                double[] sum = new double[g1.length];
                                sum[0] = g1[0] + g2[0]; //count
                                for (int i = 1; i < g1.length; i++)
                                    sum[i] = g1[i] + g2[i];

                                return sum;
                            }
                        );

            new_weights = updateWeight(weights, partial_result, iteration, 1, 0);
            Tuple2<Double, Double> norm = computeNorm(weights, new_weights);

            validation = condition(norm, max_iterations, iteration, accuracy);

            iteration++;
        }while(!validation);

        this.sparkContext
                .parallelize(Arrays.asList(weights))
                .map(
                    vector -> {
                        return Arrays
                                    .toString(vector)
                                    .replace("[", "")
                                    .replace("]", "");
                    }
                )
                .saveAsTextFile(output);

    }


    public static boolean condition(Tuple2<Double, Double> norm, int max_iterations, int current_iteration, double accuracy){
        Tuple2<Double, Double> input = norm;
        return (input.field0 < accuracy * Math.max(input.field1, 1.0) || current_iteration > max_iterations);
    }

    public static Tuple2<Double, Double> computeNorm(double[] previousWeights, double[] weights) {
        double normDiff = 0.0;
        double normWeights = 0.0;
        for (int j = 0; j < weights.length; j++) {
            normDiff += Math.abs(weights[j] - previousWeights[j]);
            normWeights += Math.abs(weights[j]);
        }
        return new Tuple2(normDiff, normWeights);
    }

    public static double[] updateWeight(double[] weights, double[] input, int current_iteration, double stepSize, double regulizer){

        double count = input[0];
        double alpha = (stepSize / (current_iteration + 1));
        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {
            newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * input[j + 1];
        }
        return newWeights;
    }


    class TransformPerPartitionSpark implements FlatMapFunction<Iterator<String>, double[]>{

        int features = 0;

        public TransformPerPartitionSpark(int features) {
            this.features = features;
        }

        @Override
        public Iterator<double[]> call(Iterator<String> lines) throws Exception {
            List<double[]> list = new ArrayList<>();
            lines.forEachRemaining(line -> {
                String[] pointStr = line.split(" ");
                double[] point = new double[features+1];
                point[0] = Double.parseDouble(pointStr[0]);
                for (int i = 1; i < pointStr.length; i++) {
                    if (pointStr[i].equals("")) {
                        continue;
                    }
                    String kv[] = pointStr[i].split(":", 2);
                    point[Integer.parseInt(kv[0])-1] = Double.parseDouble(kv[1]);
                }
                list.add(point);
            });
            return list.iterator();
        }
    }

    class ComputeLogisticGradientPerPartitionSpark implements FlatMapFunction<Iterator<double[]>, double[]>{
        double[] weights;
        double[] sumGradOfPartition;
        int features;
        boolean first = true;
        Broadcast<double[]> broadcast_weight;

        public ComputeLogisticGradientPerPartitionSpark(int features, Broadcast<double[]> broadcast_weight) {
            this.features = features;
            sumGradOfPartition = new double[features + 1]; //position 0 is for the count
            this.broadcast_weight = broadcast_weight;
        }

        @Override
        public Iterator<double[]> call(Iterator<double[]> points) throws Exception {
            if(first == true){
                open();
                first = false;
            }
            List<double[]> list = new ArrayList<>(1);
            points.forEachRemaining(point -> {
                double dot = 0;
                for (int j = 0; j < weights.length; j++)
                    dot += weights[j] * point[j + 1];
                for (int j = 0; j < weights.length; j++)
                    sumGradOfPartition[j + 1] += ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

                sumGradOfPartition[0] += 1; //counter for the step size required in the update

            });
            list.add(sumGradOfPartition);
            return list.iterator();
        }

        private void open(){
            this.weights = broadcast_weight.getValue();
            sumGradOfPartition = new double[features + 1];
        }
    }
}
