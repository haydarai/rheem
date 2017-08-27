package org.qcri.rheem.profiler.core;

import org.apache.pig.builtin.TOP;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.profiler.core.api.*;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.java.JavaOperatorProfilers;
import org.qcri.rheem.profiler.spark.SparkOperatorProfilers;
import org.qcri.rheem.profiler.spark.SparkOperatorProfiler;
import sun.security.provider.SHA;

import java.util.*;
import java.util.function.Supplier;

/**
 * Generates rheem plans for profiling.
 */
public class ProfilingPlanBuilder {

    private static List<String> ALL_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("textsource", "collectionsource", "map", "filter", "flatmap", "reduce", "globalreduce", "distinct", "distinct-string",
            "distinct-integer", "sort", "sort-string", "sort-integer", "count", "groupby", "join", "union", "cartesian", "callbacksink", "collect",
            "word-count-split", "word-count-canonicalize", "word-count-count"));

    private static List<String> SOURCE_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("textsource", "collectionsource"));

    private static List<String> Test_UNARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("reduce"));

    private static List<String> UNARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("map", "filter", "flatmap", "reduce", "globalreduce", "distinct", "distinct-string",
            "distinct-integer", "sort", "sort-string", "sort-integer", "count",
            "word-count-split", "word-count-canonicalize", "word-count-count"));

    private static List<String> BINARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("groupby", "join", "union", "cartesian"));

    private static List<String> Test_BINARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("union"));


    private static List<String> SINK_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList( "callbacksink", "collect"));

    private static Topology topology;
    private static ProfilingConfig profilingConfig;

    public static List< ? extends OperatorProfiler> PlanBuilder(Topology topology, ProfilingConfig profilingConfig){
        //topology = topology;
        //profilingConfig = profilingConfig;

        // check the topology node number
        if (topology.getNodeNumber()==1){
            return singleOperatorProfilingPlanBuilder(profilingConfig);
        } else {
            // TODO: add other topologies: pipeline, star,...
            return singleOperatorProfilingPlanBuilder(profilingConfig);
        }
    }

    public static List<List<PlanProfiler>> exhaustiveProfilingPlanBuilder(List<Shape> shapes, ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        assert (shapes.size()!=0);

        List<List<PlanProfiler>> topologyPlanProfilers = new ArrayList<>();

        for(Shape s:shapes){
            if (profilingConfig.getProfilingPlanGenerationEnumeration().equals("exhaustive")){
                topologyPlanProfilers.add(randomProfilingPlanBuilder(s,1));
            } else {
                // TODO: add more profiling plan generation enumeration: random, worstPlanGen (more execution time),betterPlanGen (less execution time)
                topologyPlanProfilers.add(randomProfilingPlanBuilder(s,1));
            }
        }
        return topologyPlanProfilers;
    }


    /**
     * Generates a list that contains a {@param numberPlans} of random {@link PlanProfiler}s for the given {@param shape}
     * @param shape
     * @return
     */
    public static List<PlanProfiler> randomProfilingPlanBuilder(Shape shape, int numberPlans){
        List<PlanProfiler> planProfilers = new ArrayList<>();
        PlanProfiler planProfiler = new PlanProfiler(shape, profilingConfig);

        for (int p=1;p<=numberPlans;p++){

            // Fill the sources
            for(Topology t:shape.getSourceTopologies()){
                prepareSource(t);
            }

            // Fill with unary operator profilers
            for(Topology t:shape.getPipelineTopologies()){
                // check if the nodes are not already filled in the source or sink
                if ((t.getNodes().isEmpty()||(!t.isSource())))
                    for(int i=1;i<=t.getNodeNumber();i++)
                    t.getNodes().push(new Tuple2<String, OperatorProfiler>("unaryNode", unaryNodeFill()));
            }

            // Fill with binary operator profilers
            for(Topology t:shape.getJunctureTopologies()){
                // check if the nodes are not already filled in the source or sink
                if (t.getNodes().isEmpty())
                    t.getNodes().push(new Tuple2<String, OperatorProfiler>("binaryNode", binaryNodeFill()));
            }


            // Fill the sinks
            //for(Topology t:shape.getSinkTopology()){
                prepareSink(shape.getSinkTopology());
            //}

            // Build the planProfiler
            buildPlanProfiler(shape);

            //planProfiler
            planProfilers.add(planProfiler);

        }
        return planProfilers;
    }

    /**
     * Generates a list of exhaustive {@link PlanProfiler}s for the given {@link Shape}
     * @param shape
     * @return
     */
    public static List<PlanProfiler> exhaustiveProfilingPlanBuilder(Shape shape){
        List<PlanProfiler> planProfilers = new ArrayList<>();
        PlanProfiler planProfiler = new PlanProfiler(shape, profilingConfig);

        // Start with the sink operator
        //shape.getSinkTopology().getNode
        return planProfilers;
    }
        /**
         * Build the plan profiler; connect the nodes and build rheem plan;
         */
    private static void buildPlanProfiler(Shape shape){

        // Start with the sink operator
        Topology sinkTopology = shape.getSinkTopology();

        // recursively connect predecessor nodes

        Stack<Tuple2<String, OperatorProfiler>> sinkNodes = (Stack<Tuple2<String, OperatorProfiler>>) sinkTopology.getNodes().clone();

        //Tuple2<String, OperatorProfiler> sinkNode = (Tuple2<String, OperatorProfiler>) sinkTopology.getNodes().iterator().next();
        Tuple2<String, OperatorProfiler> lastConnectedNode = connectNodes(sinkTopology, sinkTopology.getNodes().pop(),0);


        sinkTopology.setNodes(sinkNodes);
        // connect the lastConnectedNode with the head node of successive topology
        //for

        //preparePlanProfiler(planProfiler);

    }

    /**
     * The below method will connect nodes inside of a topology and returns the head node so to connect it with the previous topology
     * @param topology
     * @return
     */
    private static Tuple2<String,OperatorProfiler> connectNodes(Topology topology, Tuple2<String, OperatorProfiler> currentnode, int inputSlot) {

        Stack nodes = topology.getNodes();
        Tuple2<String, OperatorProfiler> previousNode = null;
        Tuple2<String, OperatorProfiler> currentNode = currentnode;

        // Loop through all the nodes
        while(!nodes.isEmpty()){
            previousNode=currentNode;
            currentNode = (Tuple2<String, OperatorProfiler>) nodes.pop();

            //check if the first input slot of the previous node is filled
            //if(previousNode.getField1().getOperator().isUnconnected()){
                // TODO: check the dataType and correct it
                //currentNode.getField1().getOperator()
                // connect previous with current node
                currentNode.getField1().getOperator().connectTo(0,previousNode.getField1().getOperator(),inputSlot);

                // Reset inputSlot to 0 after connecting the last pipeline node with the jucture topology's node
                // so to avoid error when connecting the nodes of a pipeline topology
                if (inputSlot!=0)
                    inputSlot = 0;
           // }
        }

        // at the end of this method should connect the lastConnectedNode with the head node of predecessor topology
        // recurse the predecessor tpgs
        //get the predecessors of tmp topology
        if  (!(topology.getInput(0).getOccupant()==null)){
            List<Topology> predecessors = topology.getPredecessors();
            int inputslot = 0;
            for(Topology t:predecessors){
                connectNodes(t, currentNode, inputslot);
                inputslot++;
            }

        }

        return currentNode;
    }

    private static void preparePlanProfiler(Topology topology) {
        //for(Topology t:planProfiler.ge)
    }

    /**
     * Add nodes to source Topology (i.e. pipeline topology)
     * PS: Source node i counted in the node number
     * @param topology
     */
    private static void prepareSource(Topology topology) {
        // add the first source node
        topology.getNodes().push(new Tuple2<String, OperatorProfiler>("sourceNode", sourceNodeFill()));
        // add the remaining nodes with unary nodes
        for(int i=1;i<=topology.getNodeNumber()-1;i++)
                topology.getNodes().push(new Tuple2<String, OperatorProfiler>("unaryNode", unaryNodeFill()));
    }

    /**
     * Add the sink to the sink topology; NOTE: that the sink is node included as a node
     * @param topology
     */
    private static void prepareSink(Topology topology) {
        // check if the sink is already filled as pipeline/source Topology.
        if (topology.getNodes().empty()){
            //Add the first unary nodes
            for(int i=1;i<=topology.getNodeNumber();i++)
                topology.getNodes().push(new Tuple2<String, OperatorProfiler>("unaryNode", unaryNodeFill()));
            // Add the last sink node
            topology.getNodes().push(new Tuple2<String, OperatorProfiler>("sinkNode", sinkNodeFill()));
        } else{
            topology.getNodes().push(new Tuple2<String, OperatorProfiler>("sinkNode", sinkNodeFill()));
        }

    }

    /**
     * Fills the toplogy instance with unary profiling operators
     * @return
     */
    private static OperatorProfiler sinkNodeFill(){
        // we currently support use the collection source
        return getProfilingOperator(SINK_EXECUTION_OPLERATORS.get(0));
    }

    /**
     * Fills the toplogy instance with unary profiling operators
     * @return
     */
    private static OperatorProfiler sourceNodeFill(){
        // we currently support collection source
        return getProfilingOperator(SOURCE_EXECUTION_OPLERATORS.get(1));
    }

    /**
     * Fills the toplogy instance with unary profiling operators
     * @return
     */
    private static OperatorProfiler unaryNodeFill(){
        int rnd = (int)(Math.random() * Test_UNARY_EXECUTION_OPLERATORS.size());
        return getProfilingOperator(Test_UNARY_EXECUTION_OPLERATORS.get(rnd));
    }

    /**
     * Fills the toopology instance with binary profiling operator
     * @return
     */
    private static OperatorProfiler binaryNodeFill(){
        int rnd = (int)(Math.random() * Test_BINARY_EXECUTION_OPLERATORS.size());
        return getProfilingOperator(Test_BINARY_EXECUTION_OPLERATORS.get(rnd));
    }

    /**
     * Builds all possible combinations of profiling plans of the input {@link Topology}ies
     * @param topologies
     * @param profilingConfiguration
     * @return List of all sink profiling plans
     */

    public static List<List<OperatorProfiler>> exhaustiveProfilingTopologyPlanBuilder(List<Topology> topologies,ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        assert (topologies.size()!=0);

        List<List<OperatorProfiler>> topologyPlanProfilers = new ArrayList<>();

        for(Topology t:topologies){
            if (profilingConfig.getProfilingPlanGenerationEnumeration().equals("exhaustive")){
                topologyPlanProfilers.add(singleExhaustiveProfilingPlanBuilder2(t));
            } else {
                // TODO: add more profiling plan generation enumeration: random, worstPlanGen (more execution time),betterPlanGen (less execution time)
                topologyPlanProfilers.add(singleExhaustiveProfilingPlanBuilder2(t));
            }
        }
        return topologyPlanProfilers;
    }



    public static List<OperatorProfiler> singleExhaustiveProfilingPlanBuilder2(Topology topology){
        List<OperatorProfiler> planProfilers = null;

        return planProfilers;
    }





    /**
     * Builds a profiling plan for a pipeline Topology
     * @param shape
     * @param profilingConfiguration
     * @return
     */
    public static List<PlanProfiler> pipelineProfilingPlanBuilder(Shape shape,ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        if (profilingConfig.getProfilingPlanGenerationEnumeration().equals("exhaustive")){
            return exhaustivePipelineProfilingPlanBuilder(shape);
        } else {
            // TODO: add more profiling plan generation enumeration: random, worstPlanGen (more execution time),betterPlanGen (less execution time)
            return exhaustivePipelineProfilingPlanBuilder(shape);
        }
    }

    /**
     * To remove
     * @param shape
     * @param profilingConfiguration
     * @return
     */
    public static List<PlanProfiler> exhaustiveProfilingPlanBuilder(Shape shape,ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        if (profilingConfig.getProfilingPlanGenerationEnumeration().equals("exhaustive")){
            return exhaustivePipelineProfilingPlanBuilder(shape);
        } else {
            // TODO: add more profiling plan generation enumeration: random, worstPlanGen (more execution time),betterPlanGen (less execution time)
            return exhaustivePipelineProfilingPlanBuilder(shape);
        }
    }

    /**
     * Still for test
     * @param shape
     * @return
     */

    private static List<PlanProfiler> exhaustivePipelineProfilingPlanBuilder(Shape shape) {
        List<PlanProfiler> profilingPlans = new ArrayList<>();

        PlanProfiler planProfiler = new PlanProfiler(shape, profilingConfig);
        // Set the pla Profiler

            // Set source operator profiler
            planProfiler.setSourceOperatorProfiler(SparkOperatorProfilers.createSparkCollectionSourceProfiler(1,String.class));
            // Set unary operator profiler
            planProfiler.setUnaryOperatorProfilers(Arrays.asList(SparkOperatorProfilers.createSparkMapProfiler(1,3)));
            // Set sink operator profiler
            planProfiler.setSinkOperatorProfiler(SparkOperatorProfilers.createSparkLocalCallbackSinkProfiler(1));

        profilingPlans.add(planProfiler);
        return profilingPlans;
    }

    /**
     * Builds an operator profiling specifically for single operator profiling with fake jobs
     * @param profilingConfig
     * @return
     */
    public static List<? extends OperatorProfiler> singleOperatorProfilingPlanBuilder(ProfilingConfig profilingConfig){
        if (profilingConfig.getProfilingPlanGenerationEnumeration().equals("exhaustive")){
            return exhaustiveSingleOperatorProfilingPlanBuilder(profilingConfig.getProfilingPlateform());
        } else {
            // TODO: add more profiling plan generation enumeration: random, worstPlanGen (more execution time),betterPlanGen (less execution time)
            return exhaustiveSingleOperatorProfilingPlanBuilder(profilingConfig.getProfilingPlateform());
        }
    }

    /**
     * Builds an operator profiling specifically for single operator profiling with fake jobs
     * @param Plateform
     * @return
     */
    private static List<? extends OperatorProfiler> exhaustiveSingleOperatorProfilingPlanBuilder(String Plateform) {
        List<String> operators = new ArrayList<String>(Arrays.asList("textsource","map", "collectionsource",  "filter", "flatmap", "reduce", "globalreduce", "distinct", "distinct-string",
                "distinct-integer", "sort", "sort-string", "sort-integer", "count", "groupby", "join", "union", "cartesian", "callbacksink", "collect",
                "word-count-split", "word-count-canonicalize", "word-count-count"));
        List<SparkOperatorProfiler> profilingOperators = new ArrayList<>();
        //List allCardinalities = this.profilingConfig.getInputCardinality();
        //List dataQuata = this.profilingConfig.getDataQuantaSize();
        //List UdfComplexity = this.profilingConfig.getUdfsComplexity();

        for (String operator : operators) {
            switch (operator) {
                case "textsource":
                    profilingOperators.add(SparkOperatorProfilers.createSparkTextFileSourceProfiler(1));
                    break;
                case "collectionsource":
                    profilingOperators.add(SparkOperatorProfilers.createSparkCollectionSourceProfiler(1));
                    break;
                case "map":
                    profilingOperators.add(SparkOperatorProfilers.createSparkMapProfiler(1, 1));
                    break;
                case "filter":
                    profilingOperators.add(SparkOperatorProfilers.createSparkFilterProfiler(1, 1));
                    break;
                case "flatmap":
                    profilingOperators.add(SparkOperatorProfilers.createSparkFlatMapProfiler(1, 1));
                    break;
                case "reduce":
                    profilingOperators.add(SparkOperatorProfilers.createSparkReduceByProfiler(1, 1));
                    break;
                case "globalreduce":
                    profilingOperators.add(SparkOperatorProfilers.createSparkGlobalReduceProfiler(1, 1));
                    break;
                case "distinct":
                case "distinct-string":
                    profilingOperators.add(SparkOperatorProfilers.createSparkDistinctProfiler(1));
                    break;
                case "distinct-integer":
                    profilingOperators.add(SparkOperatorProfilers.createSparkDistinctProfiler(
                            DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                            Integer.class,
                            new Configuration()
                    ));
                    break;
                case "sort":
                case "sort-string":
                    profilingOperators.add(SparkOperatorProfilers.createSparkSortProfiler(1));
                    break;
                case "sort-integer":
                    profilingOperators.add(SparkOperatorProfilers.createSparkSortProfiler(
                            DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                            Integer.class,
                            new Configuration()
                    ));
                    break;
                case "count":
                    profilingOperators.add(SparkOperatorProfilers.createSparkCountProfiler(1));
                    break;
                case "groupby":
                    profilingOperators.add(SparkOperatorProfilers.createSparkMaterializedGroupByProfiler(1, 1));
                    break;
                case "join":
                    profilingOperators.add(SparkOperatorProfilers.createSparkJoinProfiler(1, 1));
                    break;
                case "union":
                    profilingOperators.add(SparkOperatorProfilers.createSparkUnionProfiler(1));
                    break;
                case "cartesian":
                    profilingOperators.add(SparkOperatorProfilers.createSparkCartesianProfiler(1));
                    break;
                case "callbacksink":
                    profilingOperators.add(SparkOperatorProfilers.createSparkLocalCallbackSinkProfiler(1));
                    break;
            }
        }
        return profilingOperators;
    }

    /**
     * Get the {@link OperatorProfiler} of the input string
     * @param operator
     * @return
     */
    private static OperatorProfiler getProfilingOperator(String operator) {
        //List allCardinalities = this.profilingConfig.getInputCardinality();
        //List dataQuata = this.profilingConfig.getDataQuantaSize();
        //List UdfComplexity = this.profilingConfig.getUdfsComplexity();

        if (profilingConfig.getProfilingPlateform().equals("spark")){
            switch (operator) {
                case "textsource":
                    return SparkOperatorProfilers.createSparkTextFileSourceProfiler(1);
                case "collectionsource":
                    return SparkOperatorProfilers.createSparkCollectionSourceProfiler(1);
                case "map":
                    return SparkOperatorProfilers.createSparkMapProfiler(1, 1);
                case "filter":
                    return (SparkOperatorProfilers.createSparkFilterProfiler(1, 1));
                case "flatmap":
                    return (SparkOperatorProfilers.createSparkFlatMapProfiler(1, 1));
                case "reduce":
                    return (SparkOperatorProfilers.createSparkReduceByProfiler(1, 1));
                case "globalreduce":
                    return (SparkOperatorProfilers.createSparkGlobalReduceProfiler(1, 1));

                case "distinct":
                case "distinct-string":
                    return (SparkOperatorProfilers.createSparkDistinctProfiler(1));

                case "distinct-integer":
                    return (SparkOperatorProfilers.createSparkDistinctProfiler(
                            DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                            Integer.class,
                            new Configuration()
                    ));

                case "sort":
                case "sort-string":
                    return (SparkOperatorProfilers.createSparkSortProfiler(1));

                case "sort-integer":
                    return (SparkOperatorProfilers.createSparkSortProfiler(
                            DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                            Integer.class,
                            new Configuration()
                    ));

                case "count":
                    return (SparkOperatorProfilers.createSparkCountProfiler(1));

                case "groupby":
                    return (SparkOperatorProfilers.createSparkMaterializedGroupByProfiler(1, 1));

                case "join":
                    return (SparkOperatorProfilers.createSparkJoinProfiler(1, 1));

                case "union":
                    return (SparkOperatorProfilers.createSparkUnionProfiler(1));

                case "cartesian":
                    return (SparkOperatorProfilers.createSparkCartesianProfiler(1));

                case "callbacksink":
                    return (SparkOperatorProfilers.createSparkLocalCallbackSinkProfiler(1));

                default:
                    System.out.println("Unknown operator: " + operator);
                    return (SparkOperatorProfilers.createSparkLocalCallbackSinkProfiler(1));
            }
        } else{
            switch (operator) {
                case "textsource":
                    return (JavaOperatorProfilers.createJavaTextFileSourceProfiler(1));

                case "collectionsource":
                    return (JavaOperatorProfilers.createJavaCollectionSourceProfiler(1));

                case "map":
                    return (JavaOperatorProfilers.createJavaMapProfiler(1, 1));

                case "filter":
                    return (JavaOperatorProfilers.createJavaFilterProfiler(1, 1));
                case "flatmap":
                    return (JavaOperatorProfilers.createJavaFlatMapProfiler(1, 1));

                case "reduce":
                    return (JavaOperatorProfilers.createJavaReduceByProfiler(1, 1));

                case "globalreduce":
                    return (JavaOperatorProfilers.createJavaGlobalReduceProfiler(1, 1));

                case "distinct":
                case "distinct-string":
                    return (JavaOperatorProfilers.createJavaDistinctProfiler(1));

                case "distinct-integer":
                    return (JavaOperatorProfilers.createJavaDistinctProfiler(
                            DataGenerators.createReservoirBasedIntegerListSupplier(new ArrayList<List<Integer>>(), 0.0, new Random(), 1),
                            List.class
                    ));

                case "sort":
                case "sort-string":
                    return (JavaOperatorProfilers.createJavaSortProfiler(1, 1));

                case "sort-integer":
                    return (JavaOperatorProfilers.createJavaSortProfiler(
                            DataGenerators.createReservoirBasedIntegerListSupplier(new ArrayList<List<Integer>>(), 0.0, new Random(), 1),
                            List.class
                    ));

                case "count":
                    return (JavaOperatorProfilers.createJavaCountProfiler(1));

                case "groupby":
                    return (JavaOperatorProfilers.createJavaMaterializedGroupByProfiler(1, 1));

                case "join":
                    return (JavaOperatorProfilers.createJavaJoinProfiler(1, 1));

                case "union":
                    return (JavaOperatorProfilers.createJavaUnionProfiler(1));

                case "cartesian":
                    return (JavaOperatorProfilers.createJavaCartesianProfiler(1));

                case "callbacksink":
                    return (JavaOperatorProfilers.createJavaLocalCallbackSinkProfiler(1));

                case "collect":
                    return (JavaOperatorProfilers.createCollectingJavaLocalCallbackSinkProfiler(1));

                case "word-count-split": {
                    final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2 + 1, 10 + 1, new Random(42));
                    return (
                            JavaOperatorProfilers.createJavaFlatMapProfiler(
                                    () -> String.format("%s %s %s %s %s %s %s %s %s",
                                            randomStringSupplier.get(), randomStringSupplier.get(),
                                            randomStringSupplier.get(), randomStringSupplier.get(),
                                            randomStringSupplier.get(), randomStringSupplier.get(),
                                            randomStringSupplier.get(), randomStringSupplier.get(),
                                            randomStringSupplier.get()),
                                    str -> Arrays.asList(str.split(" ")),
                                    String.class,
                                    String.class
                            ));

                }
                case "word-count-canonicalize": {
                    final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2 + 1, 10 + 1, new Random(42));
                    return (
                            JavaOperatorProfilers.createJavaMapProfiler(
                                    randomStringSupplier,
                                    word -> new Tuple2<>(word.toLowerCase(), 1),
                                    String.class,
                                    Tuple2.class
                            )
                    );

                }
                default:
                    System.out.println("Unknown operator: " + operator);
                    return (JavaOperatorProfilers.createJavaLocalCallbackSinkProfiler(1));
            }
        }
    }

}
