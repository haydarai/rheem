package org.qcri.rheem.experiment.kmeans;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;


final public class KmeansSparkImplementation extends SparkImplementation {
    public KmeansSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        int n_centroid=((VariableParameter<Integer>)parameters.getParameter("n_centroid")).getVariable();
        long seed=((VariableParameter<Long>)parameters.getParameter("seed")).getVariable();
        int iterations = ((VariableParameter<Integer>)parameters.getParameter("iterations")).getVariable();

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        RDD<Vector> points = this.sparkContext.textFile(input)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            try {
                                return Vectors.dense(Double.parseDouble(fields[0]), Double.parseDouble(fields[1]));
                            } catch (Exception e) {
                                return Vectors.dense(-1, -1);
                            }
                        }
                ).rdd();

        KMeansModel clusters = (KMeansModel) KMeans.train(points, n_centroid, iterations);

        clusters.save(this.sparkContext.sc(), output);
    }

}
