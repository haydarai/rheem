package org.qcri.rheem.experiment.kmeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

final public class KmeansFlinkImplementation extends FlinkImplementation {
    public KmeansFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
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
        MapOperator<String, Point> points = this.env.readTextFile(input).map(
            line -> {
                String[] fields = line.split(",");
                try {
                    final Point point = new Point(Double.parseDouble(fields[0]), Double.parseDouble(fields[1]));
                    return point;
                } catch (Exception e) {
                    return new Point(-1, -1);
                }
            }
        );
        ArrayList<Centroid> list = new ArrayList<>(n_centroid);
        Random random = new Random(seed);
        for(int i = 0; i < n_centroid; i++){
            list.add(new Centroid(random.nextInt(), random.nextGaussian(), random.nextGaussian()));
        }

        DataSet<Centroid> centroids = this.env.fromCollection(list);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(iterations);

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        clusteredPoints.writeAsText(output);
    }



    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @FunctionAnnotation.ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    @FunctionAnnotation.ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }
}
