package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by migiwara on 16/07/17.
 */
public class Shape {

    //TODO: Add a vectorlog nested class for more readablilty purposes
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    // subshapes that will have all exhaustive filled with different nodes;plateforms;Types of the same shape
    private List<Shape> subShapes = new ArrayList<>();
    private List<Topology> allTopologies = new ArrayList<>();
    private List<Topology> sourceTopologies = new ArrayList<>();
    private String plateform;
    private List<PipelineTopology> pipelineTopologies = new ArrayList<>();
    private List<JunctureTopology> junctureTopologies = new ArrayList<>();
    private List<LoopTopology> loopTopologies = new ArrayList<>();
    //private final int vectorSize = 105;
    private final int vectorSize = 146;
    double[] vectorLogs= new double[vectorSize-1];
    private int topologyNumber;
    private List<String> operators = new ArrayList<>();
    private List<double[]> exhaustiveVectors = new ArrayList<>();
    private int startOpPos = 4;
    HashMap<String,Integer> operatorVectorPosition = new HashMap<String,Integer>(){{
        put("Map", startOpPos);put("map", startOpPos);
        put("filter", startOpPos +7);put("FlatMap", startOpPos +14);put("flatmap", startOpPos +14);put("ReduceBy", startOpPos +21);put("reduce", startOpPos +21);put("globalreduce", startOpPos +28);
        put("distinct", startOpPos +35);put("groupby", startOpPos +42);put("sort", startOpPos +49);put("join", startOpPos +56);put("union", startOpPos +63);put("cartesian", startOpPos +70);
        put("randomsample", startOpPos +77);put("shufflesample", startOpPos +84);put("bernoullisample", startOpPos +91);put("dowhile", startOpPos +98);put("repeat", startOpPos +105);
        put("collectionsource", startOpPos +112);put("TextFileSource", startOpPos +119);put("textsource", startOpPos + 119);put("callbacksink", startOpPos +126);
        put("LocalCallbackSink", startOpPos + 126);
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
     */
    public Shape(){
    }
    /**
     * Shape Constructor that creates a shape from a sink topology then filling the shape in down to up way
     * @param topology
     */
    public Shape(Topology topology){
        this.sinkTopology = topology;

        // set the shape nodenumber
        topologyNumber = topology.getTopologyNumber();
    }
    /**
     * generate a clone of the current Shape
     */

    // TODO: is not well optimized
    public Shape clone(){
        Shape newShape = new Shape(this.sinkTopology.createCopy(this.getSinkTopology().getTopologyNumber()));
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

    public void prepareVectorLog(boolean ispreExecution){
        double[] logs = new double[vectorSize];
        // Loop through all subShapes

        logs[0]=this.getPipelineTopologies().size();
        logs[1]=this.getJunctureTopologies().size();
        logs[2]=this.getLoopTopologies().size();
        logs[3]=0;
        // Loop through all topologies
        this.allTopologies.stream()
            .forEach(t -> {
                // Loop through all nodes
                t.getNodes().stream()
                        .forEach(tuple ->{
                            int start = 4;
                            String[] operatorName = tuple.getField0().split("\\P{Alpha}+");
                            operators.add(operatorName[0]);
                            addOperatorLog(logs, t, tuple, operatorName[0],ispreExecution);
                        });
            });
        averageSelectivityComplexity(logs);
        this.setVectorLogs(logs.clone());
        // reinitialize log array every subShape
        Arrays.fill(logs, 0);
    }

    /**
     * Add log for only one operator
     * @param logs
     * @param t
     * @param tuple
     * @param s
     * @param ispreExecution
     */
    private void addOperatorLog(double[] logs, Topology t, Tuple2<String, OperatorProfiler> tuple, String s, boolean ispreExecution) {
        if (ispreExecution)
            preFillLog((OperatorProfilerBase) tuple.getField1(),logs,t,operatorVectorPosition.get(s));
        else
            fillLog(tuple,logs,t,operatorVectorPosition.get(s));
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
        // TODO: if the operator is inside pipeline and the pipeline is ainside a loop body then the operator should be put as pipeline and loop
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
            // average selectivity of non source/sink/loop operators
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
        // TODO: if the operator is inside pipeline and the pipeline is ainside a loop body then the operator should be put as pipeline and loop
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
            // average selectivity of non source/sink/loop operators
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
        vectorLogs[vectorSize-2] = (int) inputCardinality;
        vectorLogs[vectorSize-1] =  dataQuantaSize;
    }

    /**
     * Modify operator platform
     * @param operator
     * @param platform
     * @param logVector
     */
    private void modifyOperatorPlatform(String operator, String platform, double[] logVector) {
        // get operator position
        int opPos = operatorVectorPosition.get(operator);

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
        int opPos = operatorVectorPosition.get(operator);
        for(String platform:DEFAULT_PLATFORMS){
            if(logVector[opPos + plateformVectorPostion.get(platform)]==1)
                return platform;
        }
        new RheemException(String.format("couldn't find a plateform for operator %s",operator));
        return null;
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

    /**
     * Will exhaustively generate all platform filled logVectors from the input logVector
     * @param vectorLog
     * @param platform
     * @param start
     */
    public void exhaustivePlanFiller(double[] vectorLog, String platform, int start){
        // if no generated plan fill it with equal values (all oerators in first platform java)

        // clone input vectorLog
        double[] newVectorLog = vectorLog.clone();

        // Initial vectorLog filling: first platform is set for all operators
        if (exhaustiveVectors.isEmpty()){
            for(String operator:operators){
                modifyOperatorPlatform(operator,DEFAULT_PLATFORMS.get(0),newVectorLog);
            }
            exhaustiveVectors.add(newVectorLog.clone());
            exhaustivePlanFiller(newVectorLog,platform,start);
            return;
        }

        // Recursive exhaustive filling platforms for each operator
        for(int i=start; i<operators.size(); i++){
            String operator = operators.get(i);
            if(!(getOperatorPlatform(operator,vectorLog)==platform)){
                // re-clone vectorLog
                newVectorLog = vectorLog.clone();
                // update newVectorLog
                modifyOperatorPlatform(operator,platform,newVectorLog);
                // add current vector to exhaustiveVectors
                exhaustiveVectors.add(newVectorLog);
                // recurse over newVectorLog
                exhaustivePlanFiller(newVectorLog,platform,i+1);
            }
        }
    }


    public static Shape createShape(LocalCallbackSink sinkOperator){
        Shape newShape = new Shape();

        // Initiate current and predecessor operator
        Operator currentOperator = sinkOperator;
        Operator predecessorOperator = sinkOperator.getInput(0).getOccupant().getOwner();

        // Loop until the source
        while(!predecessorOperator.isSource()){
            // Handle pipeline cas
            // check if predecessor is unaryoperator
            if (predecessorOperator instanceof UnaryToUnaryOperator){
                PipelineTopology newPipelineTopology = new PipelineTopology();
                // add predecessor and current operators
                newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(currentOperator.toString(), new OperatorProfilerBase(currentOperator)));
                newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));

                while (predecessorOperator instanceof UnaryToUnaryOperator){
                    predecessorOperator = predecessorOperator.getInput(0).getOccupant().getOwner();
                    newPipelineTopology.getNodes().add(new Tuple2<String,OperatorProfiler>(predecessorOperator.toString(), new OperatorProfilerBase(predecessorOperator)));
                }
                newShape.getPipelineTopologies().add(newPipelineTopology);
                newShape.getAllTopologies().add(newPipelineTopology);
            }
            //TODO: add loop handling

            //TODO: add juncture handling
        }

        newShape.prepareVectorLog(true);
        return newShape;
    }

    public List<double[]> enumerateShape(){
        return  Arrays.asList(this.vectorLogs);
    }

    /**
     * SETTERS & GETTERS
     *
     */
    public double[] getVectorLogs() {
        return vectorLogs;
    }

    public void setVectorLogs(double[] vectorLogs) {
        this.vectorLogs = vectorLogs;
    }


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
}
