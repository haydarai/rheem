package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by migiwara on 16/07/17.
 */
public class Shape {

    //TODO: Add a vectorlog nested class for more readablilty purposes
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Configuration config;
    // subshapes that will have all exhaustive filled with different nodes;plateforms;Types of the same shape
    private List<Shape> subShapes = new ArrayList<>();
    private List<Topology> allTopologies = new ArrayList<>();
    private List<Topology> sourceTopologies = new ArrayList<>();
    private String plateform;
    private List<PipelineTopology> pipelineTopologies = new ArrayList<>();
    private List<JunctureTopology> junctureTopologies = new ArrayList<>();
    private List<LoopTopology> loopTopologies = new ArrayList<>();
    //private final int VECTOR_SIZE = 105;
    //private final int VECTOR_SIZE = 146;
    //private final int VECTOR_SIZE = 194;
    private final int VECTOR_SIZE = 213;
    double[] vectorLogs= new double[VECTOR_SIZE];
    double[][] vectorLogs2D= new double[10][VECTOR_SIZE];

    private int topologyNumber;
    private List<String> operatorNames = new ArrayList<>();
    List<List<String>> operatorNames2d = new ArrayList<>();
    List<List<String>> operatorNamesPostExecution2d = new ArrayList<>();
    private List<Junction> junctions = new ArrayList<>();
    private List<ExecutionTask> executionTasks = new ArrayList<>();
    private List<double[]> exhaustiveVectors = new ArrayList<>();
    private static int startOpPos = 4;
    private static int opPosStep = 8;
    private static int channelPosStep = 4;
    private static int maxOperatorNumber = 19;

    HashMap<String,Integer> OPERATOR_VECTOR_POSITION = new HashMap<String,Integer>(){{
        put("Map", startOpPos);put("map", startOpPos);
        put("filter", startOpPos + 1*opPosStep);put("FlatMap", startOpPos +2*opPosStep);put("flatmap", startOpPos +2*opPosStep);put("reduceby", startOpPos +3*opPosStep);
        put("reduce", startOpPos +3*opPosStep);put("globalreduce", startOpPos +4*opPosStep);put("distinct", startOpPos +5*opPosStep);put("groupby", startOpPos +6*opPosStep);
        put("sort", startOpPos +7*opPosStep);put("join", startOpPos +8*opPosStep);put("unionall", startOpPos +9*opPosStep);put("union", startOpPos +9*opPosStep);put("cartesian", startOpPos +10*opPosStep);put("randomsample", startOpPos +11*opPosStep);
        put("shufflesample", startOpPos +12*opPosStep);put("bernoullisample", startOpPos +13*opPosStep);put("dowhile", startOpPos +14*opPosStep);put("repeat", startOpPos +15*opPosStep);
        put("collectionsource", startOpPos +16*opPosStep);put("textfilesource", startOpPos +17*opPosStep);put("textsource", startOpPos + 17*opPosStep);put("callbacksink", startOpPos +18*opPosStep);
        put("LocalCallbackSink", startOpPos + 18*opPosStep);put("emptySlot", startOpPos + 19*opPosStep);
    }};

    /*static HashMap<String,Integer> CHANNEL_VECTOR_POSITION = new HashMap<String,Integer>(){{
        put("CollectionChannel", startOpPos + 140);put("StreamChannel", startOpPos + 144);put("RddChannel", startOpPos + 148);
        put("FileChannel", startOpPos + 184);
    }};

    static HashMap<String,Integer> CONVERSION_OPERATOR_VECTOR_POSITION = new HashMap<String,Integer>(){{
        put("JavaCollect", startOpPos + 152);put("JavaCollectionSource", startOpPos + 156);put("JavaObjectFileSink", startOpPos + 160);put("JavaObjectFileSource", startOpPos + 164);
        put("SparkCollect", startOpPos + 168);put("SparkCollectionSource", startOpPos + 172);put("SparkObjectFileSink", startOpPos + 176);put("SparkObjectFileSource", startOpPos + 180);
    }};
    */

    static HashMap<String,Integer> CHANNEL_VECTOR_POSITION = new HashMap<String,Integer>(){{
        put("CollectionChannel", startOpPos + (1+maxOperatorNumber)*opPosStep + 0*channelPosStep);
        put("StreamChannel", startOpPos + (1+maxOperatorNumber)*opPosStep + 1*channelPosStep);
        put("RddChannel", startOpPos + (1+maxOperatorNumber)*opPosStep + 2*channelPosStep);
        put("FileChannel", startOpPos + (1+maxOperatorNumber)*opPosStep + 3*channelPosStep);
    }};

    static HashMap<String,Integer> CONVERSION_OPERATOR_VECTOR_POSITION = new HashMap<String,Integer>(){{
        put("JavaCollect", startOpPos + (1+maxOperatorNumber)*opPosStep + 4*channelPosStep);put("JavaCollectionSource",startOpPos + (1+maxOperatorNumber)*opPosStep + 5*channelPosStep);put("JavaObjectFileSink", startOpPos + (1+maxOperatorNumber)*opPosStep + 6*channelPosStep);
        put("JavaObjectFileSource",startOpPos + (1+maxOperatorNumber)*opPosStep + 7*channelPosStep);put("SparkCollect", startOpPos + (1+maxOperatorNumber)*opPosStep + 8*channelPosStep);put("SparkCollectionSource", startOpPos + (1+maxOperatorNumber)*opPosStep + 9*channelPosStep);
        put("SparkObjectFileSink", startOpPos + (1+maxOperatorNumber)*opPosStep + 10*channelPosStep);put("SparkObjectFileSource", startOpPos + (1+maxOperatorNumber)*opPosStep + 11*channelPosStep);
    }};

    public static final List<String> DEFAULT_PLATFORMS = new ArrayList<>(Arrays.asList("Java Streams","Apache Spark"));

    HashMap<String,Integer> plateformVectorPostion = new HashMap<String,Integer>(){{
        put("Java Streams",0);
        put("Apache Spark",1);
    }};
    // TODO: Currently only single sink topology generation is supported
    private Topology sinkTopology;

    //private int nodeNumber = sinkTopologies.getNodeNumber()

    /**
     * Shape Constructor *empty
     * @param configuration
     */
    public Shape(Configuration configuration){
        this.config = configuration;
    }
    /**
     * Shape Constructor that creates a shape from a sink topology then filling the shape in down to up way
     * @param topology
     * @param configuration
     */
    public Shape(Topology topology, Configuration configuration){
        this.config = configuration;
        this.sinkTopology = topology;

        // set the shape nodenumber
        topologyNumber = topology.getTopologyNumber();
    }
    /**
     * generate a clone of the current Shape
     */

    // TODO: is not well optimized
    public Shape clone(){
        Shape newShape = new Shape(this.sinkTopology.createCopy(this.getSinkTopology().getTopologyNumber()), new Configuration());
        newShape.populateShape(newShape.getSinkTopology());
        newShape.setPlateform(this.plateform);
        return newShape;
    }

    /**
     * empty all nodes in all topologies
     */

    public void resetAllNodes(){
        for(Topology t:allTopologies)
            t.setNodes(new Stack<>());
    }


    /**
         * assign shape variables (i.e. number of pipelines; junctures; sinks;.. )
         * @param topology
         */
    public void populateShape(Topology topology){

        // Handle the case if the topology is pipeline Topology
        if (topology.isPipeline()){
            if (!this.pipelineTopologies.contains(topology))
                this.pipelineTopologies.add((PipelineTopology) topology);
            else
                // exit
                return;
            // Add to all topologies too
            this.allTopologies.add((PipelineTopology) topology);
            // get the predecessor of tmp topology
            if  (!(topology.getInput(0).getOccupant()==null)){
                List<Topology> predecessors = topology.getPredecessors();
                for(Topology t:predecessors)
                    // recurse for predecessor topologies
                    populateShape(t);
            } else {
                // This case means it's source topology
                sourceTopologies.add(topology);
            }

            // recurse the predecessor tpg
        } else if (topology.isJuncture()){
            // Handle the case if the topology is juncture Topology
            if (!this.junctureTopologies.contains(topology))
                this.junctureTopologies.add((JunctureTopology) topology);
            else
                // exit
                return;
            // Add to all topologies too
            this.allTopologies.add((JunctureTopology) topology);

            List<Topology> predecessors = topology.getPredecessors();

            //get the predecessors of tmp topology
            if  (!(predecessors.isEmpty())){
                //this.junctureTopologies.add((JunctureTopology) topology);
                for(Topology t:predecessors)
                    // recurse for predecessor topologies
                    populateShape(t);
            }else{
                // This case means it's source topology
                sourceTopologies.add(topology);
            }
        } else if (topology.isLoop()){
            if (!this.loopTopologies.contains(topology))
                this.loopTopologies.add((LoopTopology) topology);
            else
                // exit
                return;

            // Add to all topologies too
            this.allTopologies.add((LoopTopology) topology);
            List<Topology> predecessors = topology.getPredecessors();
            if (predecessors.size()==1){
                // imply it has only iteration input topology; no source topology
                this.sourceTopologies.add(topology);
            }
            if  (!(predecessors.isEmpty())){
                int index=0;
                for(Topology t:predecessors)
                    // recurse for predecessor topologies
                    populateShape(t);
            }
            // check if there's an output node connected to
        }
    }

    /**
     * Create a string to describe the shape; for printing purpose
     * @return
     */
    public String toString(){
        return"";
    }

    //int tmpstart =0;


    private static Stack<LoopHeadOperator> loopHeads = new Stack();

    /**
     * Create a preExecution shape from a sink operator
     * @param sinkOperator
     * @return
     */
    public static Shape createShape(LocalCallbackSink sinkOperator) {
        return createShape(sinkOperator,true, true);
    }

    /**
     * Create a shape from a sink operator
     * @param sinkOperator
     * @param ispreExecution
     * @return
     */
    public static Shape createShape(LocalCallbackSink sinkOperator, boolean ispreExecution, boolean prepareVectorLog){
        Shape newShape = new Shape(new Configuration());

        // Initiate current and predecessor operator
        Operator currentOperator = sinkOperator;
        Operator predecessorOperator = sinkOperator.getInput(0).getOccupant().getOwner();

        // Loop until the source
        while(!predecessorOperator.isSource()){
            // Handle pipeline cas
            // check if predecessor is unaryoperator
            if (predecessorOperator instanceof UnaryToUnaryOperator){
                PipelineTopology newPipelineTopology = new PipelineTopology();

                if(currentOperator.isSink()) {
                    newPipelineTopology.getNodes().add(new Tuple2<String, OperatorProfiler>(currentOperator.toString(), new OperatorProfilerBase(currentOperator)));
                    // add current topology as a sink topology
                    newShape.setSinkTopology(newPipelineTopology);
                }

                // add predecessor and current operatorNames
                //newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(currentOperator.toString(), new OperatorProfilerBase(currentOperator)));
                newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));


                while (predecessorOperator instanceof UnaryToUnaryOperator){
                    // add operator to new topology
                    newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                    // update current and predecessor operatorNames
                    currentOperator = predecessorOperator;
                    predecessorOperator = predecessorOperator.getInput(0).getOccupant().getOwner();

                }
                newShape.getPipelineTopologies().add(newPipelineTopology);
                newShape.getAllTopologies().add(newPipelineTopology);

                // check if the current node has source node
                if(predecessorOperator.isSource()) {
                    newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                    newShape.getSourceTopologies().add(newPipelineTopology);
                    //addSourceTopology(predecessorOp erator, newPipelineTopology);
                }
            }

            //DONE: add juncture handling
            if(predecessorOperator instanceof BinaryToUnaryOperator){
                JunctureTopology newJunctureTopology = new JunctureTopology();
                if(currentOperator.isSink()) {
                    newJunctureTopology.getNodes().add(new Tuple2<String, OperatorProfiler>(currentOperator.toString(), new OperatorProfilerBase(currentOperator)));
                    // add current topology as a sink topology
                    newShape.setSinkTopology(newJunctureTopology);
                }
                newJunctureTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                newShape.getJunctureTopologies().add(newJunctureTopology);
                newShape.getAllTopologies().add(newJunctureTopology);

                // add operator to new topology
                newJunctureTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));

                // update current and predecessor operatorNames
                currentOperator = predecessorOperator;
                predecessorOperator = predecessorOperator.getInput(0).getOccupant().getOwner();

                if(predecessorOperator.isSource()) {
                    newJunctureTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                    newShape.getSourceTopologies().add(newJunctureTopology);
                    //addSourceTopology(predecessorOp erator, newPipelineTopology);
                }
            }
            //DONE: add loop handling
            if(predecessorOperator instanceof LoopHeadOperator){
                LoopTopology newLoopTopology = new LoopTopology();
                if(currentOperator.isSink()) {
                    newLoopTopology.getNodes().add(new Tuple2<String, OperatorProfiler>(currentOperator.toString(), new OperatorProfilerBase(currentOperator)));
                    // add current topology as a sink topology
                    newShape.setSinkTopology(newLoopTopology);

                }
                newLoopTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                newShape.getLoopTopologies().add(newLoopTopology);
                newShape.getAllTopologies().add(newLoopTopology);

                // add operator to new topology
                newLoopTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));

                // update current and predecessor operatorNames
                currentOperator = predecessorOperator;

                // check if the predecessor loop has been visited before (if no we visit iterIn"1" otherwise
                // we visit InitIn"0")
                if ((loopHeads.isEmpty())||(loopHeads.peek()!=predecessorOperator)) {
                    // Add current loopHead
                    loopHeads.add((LoopHeadOperator) predecessorOperator);
                    // Get the IterIn
                    predecessorOperator = predecessorOperator.getInput(1).getOccupant().getOwner();
                } else {
                    // remove current loopHead
                    loopHeads.pop();
                    // Get the InitIn
                    predecessorOperator = predecessorOperator.getInput(0).getOccupant().getOwner();
                }

                if(predecessorOperator.isSource()) {
                    newLoopTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                    newShape.getSourceTopologies().add(newLoopTopology);
                    //addSourceTopology(predecessorOp erator, newPipelineTopology);
                }
            }
        }
        if (prepareVectorLog)
            newShape.prepareVectorLog(ispreExecution);
        return newShape;
    }

    /**
     * SETTERS & GETTERS
     *
     */

    public void setSinkTopology(Topology sinkTopology) {
        this.sinkTopology = sinkTopology;
    }

    public Topology getSinkTopology() {
        return sinkTopology;
    }
    public List<Shape> getSubShapes() {
        return subShapes;
    }

    public void setSubShapes(List<Shape> subShapes) {
        this.subShapes = subShapes;
    }


    public String getPlateform() {
        return plateform;
    }

    public void setPlateform(String plateform) {
        this.plateform = plateform;
    }

    public List<Topology> getAllTopologies() {
        return allTopologies;
    }

    public List<Topology> getSourceTopologies() {
        return sourceTopologies;
    }

    public List<PipelineTopology> getPipelineTopologies() {
        return pipelineTopologies;
    }

    public List<JunctureTopology> getJunctureTopologies() {
        return junctureTopologies;
    }

    public List<LoopTopology> getLoopTopologies() {
        return loopTopologies;
    }

    public void setLoopTopologies(List<LoopTopology> loopTopologies) {
        this.loopTopologies = loopTopologies;
    }

    public int getTopologyNumber() {
        return topologyNumber;
    }


    public void updateIteration() {
    }


    /*****************************************
     * LOG subclass: will contains all feature information fr the containing shape
     ********************************************/


    public void prepareVectorLog(boolean ispreExecution){

        double[] tmpVectorLogs1D = new double[VECTOR_SIZE];
        double[][] tmpVectorLogs2D= new double[10][VECTOR_SIZE];

        // Initialize the 2d Operator names\
        for (int i = 0; i < 10; i++) {
            operatorNames2d.add(new ArrayList<String>());
            operatorNamesPostExecution2d.add(new ArrayList<String>());
        }

        // Loop through all topologies
        this.allTopologies.stream()
                .forEach(t -> {
                    // Loop through all nodes
                    t.getNodes().stream()
                            .forEach(tuple ->{
                                int start = 4;
                                String[] operatorName = tuple.getField0().split("\\P{Alpha}+");
                                if( config.getBooleanProperty("rheem.profiler.generate2dLogs",false)){
                                    // Handle the case of 2D generated logs

                                    // add topoliges to 2d vector log
                                    //tmpVectorLogs2D[0]=tmpVectorLogs1D;
                                    // Loop through all subShapes
                                    tmpVectorLogs2D[0][0]=this.getPipelineTopologies().size();
                                    tmpVectorLogs2D[0][1]=this.getJunctureTopologies().size();
                                    tmpVectorLogs2D[0][2]=this.getLoopTopologies().size();
                                    tmpVectorLogs2D[0][3]=0;
                                    // check if there's a duplicate operator
                                    operatorNames2d.stream()
                                            .filter(list-> !list.contains(operatorName[0]))
                                            .findFirst()
                                            .map(list -> {
                                                int index = operatorNames2d.indexOf(list);

                                                list.add(operatorName[0]);
                                                addOperatorLog(tmpVectorLogs2D[index], t, tuple, operatorName[0],ispreExecution);
                                                return list;
                                            });
                                    //if (!operatorNames2d.get(0).contains(operatorName[0]))
                                }
                                //else {

                                // prepare 1D log vector in all cases
                                tmpVectorLogs1D[0]=this.getPipelineTopologies().size();
                                tmpVectorLogs1D[1]=this.getJunctureTopologies().size();
                                tmpVectorLogs1D[2]=this.getLoopTopologies().size();
                                tmpVectorLogs1D[3]=0;
                                // Handle the case of 1D generated logs
                                operatorNames.add(operatorName[0]);
                                addOperatorLog(tmpVectorLogs1D, t, tuple, operatorName[0],ispreExecution);
                                //}
                            });
                });
        averageSelectivityComplexity(tmpVectorLogs1D);

        if( config.getBooleanProperty("rheem.profiler.generate2dLogs",false)){
            // set shape's 2D vector log
            this.setVectorLogs2D(tmpVectorLogs2D.clone());
            // set shapes 1D vector log as the first row of the 2D vector log; as we will update the channels only for the first row
            this.setVectorLogs(tmpVectorLogs2D[0].clone());
        }
        // set shapes 1D vector log
        this.setVectorLogs(tmpVectorLogs1D.clone());

        // reinitialize log array every subShape
        Arrays.fill(tmpVectorLogs1D, 0);
    }

    /**
     * Add log for only one operator
     * @param logs
     * @param t
     * @param tuple
     * @param operator
     * @param ispreExecution
     */
    private void addOperatorLog(double[] logs, Topology t, Tuple2<String, OperatorProfiler> tuple, String operator, boolean ispreExecution) {
        // check if the s
        //assert (OPERATOR_VECTOR_POSITION.get(s)!=null);
        if (ispreExecution)
            preFillLog((OperatorProfilerBase) tuple.getField1(),logs,t,getOperatorVectorPosition(operator));
        else
            fillLog(tuple,logs,t, OPERATOR_VECTOR_POSITION.get(operator));

    }

    private int getOperatorVectorPosition(String operator) {
        try {
            return OPERATOR_VECTOR_POSITION.get(operator);
        } catch (Exception e){
            throw new RheemException(String.format("couldn't find position log for operator %s",operator));
        }
    }

    /**
     * Calculate average selectivity
     * @param logs
     */
    void averageSelectivityComplexity(double[] logs){
        for(int i=9; i<=97; i=i+7){
            if(logs[i-4]+logs[i-5]!=0){
                logs[i]=logs[i]/(logs[i-4]+logs[i-5]);
                logs[i+1]=logs[i+1]/(logs[i-4]+logs[i-5]);
            }
        }
    }

    /**
     * Fill {@var vectorLogs} with rheem operator parameters
     * @param operatorProfilerBase
     * @param logs
     * @param t
     * @param start
     */
    void preFillLog(OperatorProfilerBase operatorProfilerBase, double[] logs, Topology t, int start){

        //Tuple2<String,OperatorProfilerBase> tuple2 = (Tuple2<String,OperatorProfilerBase>) tuple;
        // TODO: if the operator is inside pipeline and the pipeline is inside a loop body then the operator should be put as pipeline and loop
        if (t.isPipeline())
            logs[start+2]+=1;
        else if(t.isJuncture())
            logs[start+3]+=1;
        if((t.isLoop())||(t.isLoopBody()))
            logs[start+4]+=1;

        // average complexity
        logs[start+5] += operatorProfilerBase.getUDFcomplexity();

        // average selectivity
        double  selectivity = 0;
        if ((!operatorProfilerBase.getRheemOperator().isSource())&&(!operatorProfilerBase.getRheemOperator().isSink())&&(operatorProfilerBase.getRheemOperator().isLoopHead()))
            // average selectivity of non source/sink/loop operatorNames
            selectivity= operatorProfilerBase.getRheemOperator().getOutput(0).getCardinalityEstimate().getAverageEstimate()/
                    operatorProfilerBase.getRheemOperator().getInput(0).getCardinalityEstimate().getAverageEstimate();
        else if(operatorProfilerBase.getRheemOperator().isSource())
            // case of source operator we set the selectivity to 1
            selectivity = 1;
        else if(operatorProfilerBase.getRheemOperator().isLoopHead()){
            // case of a loop head (e.g: repeat operator) we replace the selectivity with number of iterations
            LoopHeadOperator loopOperator = (LoopHeadOperator)operatorProfilerBase.getRheemOperator();
            selectivity = loopOperator.getNumExpectedIterations();
        }
        logs[start+6] += (int) selectivity;
        //TODO: duplicate and selectivity to be added
    }

    /**
     * Fill {@var vectorLogs} with execution operator parameters
     * @param tuple
     * @param logs
     * @param t
     * @param start
     */
    void fillLog(Tuple2<String,OperatorProfiler> tuple, double[] logs, Topology t, int start){
        switch (tuple.getField1().getOperator().getPlatform().getName()){
            case "Java Streams":
                logs[start]+=1;
                break;
            case "Apache Spark":
                logs[start+1]+=1;
                break;
            default:
                System.out.println("wrong plateform!");
        }
        // TODO: if the operator is inside pipeline and the pipeline is inside a loop body then the operator should be put as pipeline and loop
        if (t.isPipeline())
            logs[start+2]+=1;
        else if(t.isJuncture())
            logs[start+3]+=1;
        if((t.isLoop())||(t.isLoopBody()))
            logs[start+4]+=1;

        // average complexity
        logs[start+5] += tuple.getField1().getUDFcomplexity();

        // average selectivity
        double  selectivity = 0;
        if ((!tuple.getField1().getOperator().isSource())&&(!tuple.getField1().getOperator().isSink())&&(!tuple.getField1().getOperator().isLoopHead()))
            // average selectivity of non source/sink/loop operatorNames
            selectivity= tuple.getField1().getOperator().getOutput(0).getCardinalityEstimate().getAverageEstimate()/
                    tuple.getField1().getOperator().getInput(0).getCardinalityEstimate().getAverageEstimate();
        else if(tuple.getField1().getOperator().isSource())
            // case of source operator we set the selectivity to 1
            selectivity = 1;
        else if(tuple.getField1().getOperator().isLoopHead()){
            // case of a loop head (e.g: repeat operator) we replace the selectivity with number of iterations
            LoopHeadOperator loopOperator = (LoopHeadOperator)tuple.getField1().getOperator();
            selectivity = loopOperator.getNumExpectedIterations();
        }
        logs[start+6] += (int) selectivity;
        //TODO: duplicate and selectivity to be added
    }

    public void setcardinalities(long inputCardinality, int dataQuantaSize) {
        if( config.getBooleanProperty("rheem.profiler.generate2dLogs",false)){
            vectorLogs2D[0][VECTOR_SIZE - 2] = (int) inputCardinality;
            vectorLogs2D[0][VECTOR_SIZE - 1] = dataQuantaSize;
        }

        vectorLogs[VECTOR_SIZE - 2] = (int) inputCardinality;
        vectorLogs[VECTOR_SIZE - 1] = dataQuantaSize;
    }

    /**
     * Modify operator cost
     * @param operator
     * @param cost
     * @param logVector
     */
    private void updateOperatorOutputCardinality(String operator, double cost, double[] logVector) {
        // get operator position
        int opPos = getOperatorVectorPosition(operator);

        // update cost
        logVector[opPos + 7] = cost;
    }

    /**
     * Modify operator cost
     * @param operator
     * @param cost
     * @param logVector
     */
    private void updateOperatorInputCardinality(String operator, double cost, double[] logVector) {
        // get operator position
        int opPos = getOperatorVectorPosition(operator);

        // update cost
        logVector[opPos + 6] = cost;
    }

    /**
     * Modify operator platform
     * @param operator
     * @param platform
     * @param logVector
     */
    private void updateOperatorPlatform(String operator, String platform, double[] logVector) {
        // get operator position
        int opPos = getOperatorVectorPosition(operator);
        // reset all platforms to zero
        plateformVectorPostion.entrySet().stream().
                forEach(tuple->logVector[opPos + tuple.getValue()] = 0);

        // update platform
        logVector[opPos + plateformVectorPostion.get(platform)] += 1;
    }

    /**
     * get operator platform
     * @param operator
     * @param logVector
     */
    private String getOperatorPlatform(String operator, double[] logVector) {
        // get operator position
        int opPos = OPERATOR_VECTOR_POSITION.get(operator);
        for (String platform : DEFAULT_PLATFORMS) {
            if (logVector[opPos + getPlatformVectorPosition(platform)] == 1)
                return platform;
        }
        return null;
    }

    public String[] platformVector = new String[100];

    private List<String[]> exhaustivePlatformVectors = new ArrayList<>();

    /**
     * Will exhaustively generate all platform filled logVectors from the input logVector; and update  the platform vector will be used in the runner
     * @param vectorLog
     * @param platform
     * @param start
     */
    public void exhaustivePlanFiller(double[] vectorLog,String[] platformVector, String platform, int start){
        // if no generated plan fill it with equal values (all oerators in first platform java)

        // clone input vectorLog
        double[] newVectorLog = vectorLog.clone();
        String[] newPlatformLog = platformVector.clone();

        // Initial vectorLog filling: first platform is set for all operatorNames
        if (exhaustiveVectors.isEmpty()){
            int iteration=0;
            for(String operator: operatorNames){
                updateOperatorPlatform(operator,DEFAULT_PLATFORMS.get(0),newVectorLog);
                updatePlatformVector(iteration,DEFAULT_PLATFORMS.get(0),newPlatformLog);
                iteration++;
            }
            exhaustiveVectors.add(newVectorLog.clone());
            exhaustivePlatformVectors.add(newPlatformLog.clone());
            exhaustivePlanFiller(newVectorLog,newPlatformLog, platform,start);
            return;
        }

        // Recursive exhaustive filling platforms for each operator
        for(int i = start; i< operatorNames.size(); i++){
            String operator = operatorNames.get(i);
            if(!(getOperatorPlatform(operator,vectorLog)==platform)){
                // re-clone vectorLog
                newVectorLog = vectorLog.clone();
                // re-clone platformVector
                newPlatformLog = platformVector.clone();

                // update newVectorLog
                updateOperatorPlatform(operator,platform,newVectorLog);
                updatePlatformVector(i,platform,newPlatformLog);

                // add current vector to exhaustiveVectors
                exhaustiveVectors.add(newVectorLog);
                exhaustivePlatformVectors.add(newPlatformLog);
                // recurse over newVectorLog
                exhaustivePlanFiller(newVectorLog,newPlatformLog , platform,i+1);
            }
        }
    }

    private void updatePlatformVector(int iteration, String platform, String[] newPlatformLog) {
        // update platform
        newPlatformLog[iteration] = platform;
    }


    /**
     * Logging {@link Shape}'s vector log
     */
    public void printLog() {
        final String[] outputVector = {""};
        NumberFormat nf = new DecimalFormat("##.#");
        Arrays.stream(vectorLogs).forEach(d -> {
            outputVector[0] = outputVector[0].concat( nf.format( d) + " ");
        });
        this.logger.info("Current rheem plan feature vector: " + outputVector[0]);
    }

    /**
     * Logging {@link Shape}'s enumerated vector logs
     */
    public void printEnumeratedLogs() {
        final String[] outputVector = {""};
        NumberFormat nf = new DecimalFormat("##.#");
        for(double[] vectorLog:exhaustiveVectors) {
            outputVector[0] = "";
            Arrays.stream(vectorLog).forEach(d -> {
                outputVector[0] = outputVector[0].concat(nf.format(d) + " ");
            });
            this.logger.info("Current rheem plan feature vector: " + outputVector[0]);
        }
    }


    public void updateChannels(Map<OutputSlot<?>, Junction> nodes) {
        nodes.values().stream()
                .forEach(junction -> {
                    // add junctions
                    junctions.add(junction);

                    // handle conversion tastks
                    junction.getConversionTasks().stream()
                            .forEach( conversionOperator-> {
                                executionTasks.add(conversionOperator);
                                addConversionOperator(conversionOperator);
                                addChannelLog(conversionOperator.getOutputChannel(0));
                            });

                    // add output channels
                    junction.getTargetChannels().stream().
                            forEach(outChannel->addChannelLog(outChannel));

                });

    }

    private void addConversionOperator(ExecutionTask conversionOperator) {
        String[] channelName = conversionOperator.toString().split("\\P{Alpha}+");
        // Each channel has 4 encoding digits as follow (number, consumer, producer, conversion)
        int channelStartPosition = getconversionOperatorVectorPosition(channelName[1]);
        vectorLogs[channelStartPosition]+=1;
    }

    private int getconversionOperatorVectorPosition(String conversionOperator) {
        try{
            return CONVERSION_OPERATOR_VECTOR_POSITION.get(conversionOperator);
        } catch (Exception e){
            throw new RheemException(String.format("couldn't find channel log vector offset %s",conversionOperator));
        }
    }

    private void addChannelLog(Channel outChannel) {
        String[] channelName = outChannel.toString().split("\\P{Alpha}+");
        // Each channel has 4 encoding digits as follow (number, consumer, producer, conversion)
        int channelStartPosition = getJunctureVectorPosition(channelName[0]);
        if( config.getBooleanProperty("rheem.profiler.generate2dLogs",false)){
            // add the channel log to the first row
            vectorLogs2D[0][channelStartPosition]+=1;
        }
        vectorLogs[channelStartPosition]+=1;
    }

    private int getJunctureVectorPosition(String channelName) {
        try{
            return CHANNEL_VECTOR_POSITION.get(channelName);
        } catch (Exception e){
        throw new RheemException(String.format("couldn't find channel log vector offset %s",channelName));
        }
    }

    private void addJunctureLog(Junction junction) {

    }

    /**
     * get platform log offset in a log vector
     * @param platform
     * @return
     */
    private int getPlatformVectorPosition(String platform) {
        try {
            return plateformVectorPostion.get(platform);
        } catch (Exception e){
            throw new RheemException(String.format("couldn't find a plateform for operator %s",platform));
        }
    }

    /**
     * update with execution operator informations (plateform, output cardinality)
     * @param localOperatorContexts
     */
    public void updateExecutionOperators(Map<Operator, OptimizationContext.OperatorContext> localOperatorContexts) {
        localOperatorContexts.keySet().stream()
            .forEach(operator -> {
                // We discard composite operators
                if(operator.isElementary()) {
                    String[] operatorName = new String[1];
                    double averageOutputCardinality = 0;
                    double averageInputCardinality = 0;
                    if(operator.isExecutionOperator()){
                        operatorName[0]=operator.toString().split("\\P{Alpha}+")[0]
                                .toLowerCase().replace("java","").replace("spark","");
                    } else {
                        Set<Platform> platform = operator.getTargetPlatforms();
                        operatorName[0] = operator.toString().split("\\P{Alpha}+")[0];
                        platform.stream().forEach(p->updateOperatorPlatform(operatorName[0],p.getName(),vectorLogs) );
                    }

                    if (localOperatorContexts.get(operator).getOutputCardinalities().length!=0){
                        averageOutputCardinality = localOperatorContexts.get(operator).getOutputCardinality(0).getAverageEstimate();
                        averageInputCardinality = Arrays.stream(localOperatorContexts.get(operator).getInputCardinalities())
                                .map(incard->(double)incard.getAverageEstimate())
                                .reduce(0.0,(av1,av2)->av1+av2);

                        Arrays.stream(localOperatorContexts.get(operator).getInputCardinalities()).forEach(incard1->incard1.getAverageEstimate());
                        // update shape's vector log with output cardinality
                        //updateOperatorOutputCardinality(operatorName[0],averageOutputCardinality,vectorLogs);
                        if(config.getBooleanProperty("rheem.profiler.generate2dLogs",false)){
                            double finalAverageOutputCardinality = averageOutputCardinality;
                            double finalAverageInputCardinality = averageInputCardinality;
                            operatorNamesPostExecution2d.stream()
                                    .filter(list-> !list.contains(operatorName[0]))
                                    .findFirst()
                                    .map(list -> {
                                        int index = operatorNamesPostExecution2d.indexOf(list);
                                        list.add(operatorName[0]);
                                        // update shape's vector log with output cardinality
                                        updateOperatorOutputCardinality(operatorName[0], finalAverageOutputCardinality,vectorLogs2D[index]);
                                        // update shape's vector log with target platform
                                        updateOperatorInputCardinality(operatorName[0], finalAverageInputCardinality,vectorLogs2D[index]);
                                        return list;
                                    });

                            //localOperatorContexts.get
                        }
                        //else {
                        // Update 1d vector log
                        updateOperatorOutputCardinality(operatorName[0],averageOutputCardinality,vectorLogs);
                        // update shape's vector log with target platform
                        updateOperatorInputCardinality(operatorName[0],averageInputCardinality,vectorLogs);
                        //localOperatorContexts.get
                        //}

                    }

                }
            });
    }



    // GETTERS AND SETTERS
    public double[] getVectorLogs() {
        return vectorLogs;
    }

    public double[][] getVectorLogs2D() {
        return vectorLogs2D;
    }

    public void setVectorLogs(double[] vectorLogs) {
        this.vectorLogs = vectorLogs;
    }

    public void setVectorLogs2D(double[][] vectorLogs2D) {
        this.vectorLogs2D = vectorLogs2D;
    }

    public void reinitializeLog() {
        operatorNames = new ArrayList<>();
        operatorNames2d = new ArrayList<>();
        operatorNamesPostExecution2d = new ArrayList<>();

        vectorLogs2D = new double[10][VECTOR_SIZE];
        vectorLogs = new double[VECTOR_SIZE];
    }
}
