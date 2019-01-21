package org.qcri.rheem.experiment.crocopr;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;
import org.qcri.rheem.experiment.crocopr.udf.ParserDBPedia;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;


final public class CrocoprFlinkImplementation extends FlinkImplementation {

    private static final float DAMPENING_FACTOR = 0.85f;
    private static final float EPSILON = 0.001f;

    public CrocoprFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {

        String input = ((FileParameter) parameters.getParameter("input")).getPath();
        String input2 = ((FileParameter) parameters.getParameter("input2")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        MapOperator<String, Tuple2<String, String>> file1 = readInput(input);
        MapOperator<String, Tuple2<String, String>> file2 = readInput(input2);

        DistinctOperator<Tuple2<String, String>> alls_links = file1.union(file2)
                                                                   .distinct();


        DataSet<Tuple2<Long, String>> vertextid = DataSetUtils.zipWithUniqueId(
                alls_links.flatMap(
                    new FlatMapFunction<Tuple2<String, String>, String>() {
                        @Override
                        public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                            out.collect(value.f0);
                            out.collect(value.f1);
                        }
                    }
                )
                .distinct()
        );

        DataSet<Tuple2<Long, Long>> edges = alls_links
            .join(vertextid)
            .where(1)
            .equalTo(1)
            .map(tuple -> new Tuple2<Long, String>(tuple.f1.f0, tuple.f0.f1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, String>>(){}))
            .join(vertextid)
            .where(1)
            .equalTo(1)
            .map(tuple -> new Tuple2<>(tuple.f0.f0, tuple.f1.f0))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));

        pagerank(edges, 10)
            .join(vertextid)
            .where(0)
            .equalTo(0)
            .map(tuple -> new Tuple2<>(tuple.f1.f1, tuple.f0.f1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){}))
            .writeAsText(output);
    }


    private MapOperator<String, Tuple2<String, String>> readInput(String path){

        return this.env.readTextFile(path)
                .filter(line -> !line.startsWith("#"))
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    ParserDBPedia parser = new ParserDBPedia();

                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] elements = parser.components(value);
                        return new Tuple2<>(elements[0], elements[1]);
                    }
                });
    }

    private DataSet<Tuple2<Long, Double>> pagerank(DataSet<Tuple2<Long, Long>> dataSetInputReal, int num_iteration){
        FlatMapFunction<Tuple2<Long, Long>, Long> flatMapFunction = new FlatMapFunction<Tuple2<Long, Long>, Long>() {
            @Override
            public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Long> collector) throws Exception {
                collector.collect(longLongTuple2.f0);
                collector.collect(longLongTuple2.f1);
            }
        };

        final DataSet<Long> pages = dataSetInputReal.flatMap(flatMapFunction).distinct();

        int numPages = 0;
        try {
            numPages = (int) pages.count();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // get input data
        DataSet<Long> pagesInput = pages;
        DataSet<Tuple2<Long, Long>> linksInput = dataSetInputReal;

        // assign initial rank to pages
        DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
                map(new RankAssigner((1.0d / numPages)));

        // build adjacency list from link input
        DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
                linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

        // set iterative data set
        IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(num_iteration);

        DataSet<Tuple2<Long, Double>> newRanks = iteration
                // join pages with outgoing edges and distribute rank
                .join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
                // collect and sum ranks
                .groupBy(0).aggregate(SUM, 1)
                // apply dampening factor
                .map(new Dampener(DAMPENING_FACTOR, numPages));

        DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
                newRanks,
                newRanks.join(iteration).where(0).equalTo(0)
                        // termination condition
                        .filter(new EpsilonFilter()));

        return finalPageRanks;

    }

// *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * A map function that assigns an initial rank to all pages.
     */
    public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
        Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(double rank) {
            this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(Long page) {
            outPageWithRank.f0 = page;
            return outPageWithRank;
        }
    }

    /**
     * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
     * originate. Run as a pre-processing step.
     */
    public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

        private final ArrayList<Long> neighbors = new ArrayList<Long>();

        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
            neighbors.clear();
            Long id = 0L;

            for (Tuple2<Long, Long> n : values) {
                id = n.f0;
                neighbors.add(n.f1);
            }
            out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
        }
    }

    /**
     * Join function that distributes a fraction of a vertex's rank to all neighbors.
     */
    public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
            Long[] neighbors = value.f1.f1;
            double rank = value.f0.f1;
            double rankToDistribute = rank / ((double) neighbors.length);

            for (Long neighbor: neighbors) {
                out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
            }
        }
    }

    /**
     * The function that applies the page rank dampening formula.
     */
    public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices) {
            this.dampening = dampening;
            this.randomJump = (1 - dampening) / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
            value.f1 = (value.f1 * dampening) + randomJump;
            return value;
        }
    }

    /**
     * Filter that filters vertices where the rank difference is below a threshold.
     */
    public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
            return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
        }
    }

}
