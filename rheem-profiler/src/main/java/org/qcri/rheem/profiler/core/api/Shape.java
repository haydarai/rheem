package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;

import java.util.*;

/**
 * Created by migiwara on 16/07/17.
 */
public class Shape {

    // subshapes that will have all exhaustive filled with different nodes;plateforms;Types of the same shape
    private List<Shape> subShapes = new ArrayList<>();
    private List<Topology> allTopologies = new ArrayList<>();
    private List<Topology> sourceTopologies = new ArrayList<>();
    private String plateform;
    private List<PipelineTopology> pipelineTopologies = new ArrayList<>();
    private List<JunctureTopology> junctureTopologies = new ArrayList<>();
    private List<LoopTopology> loopTopologies = new ArrayList<>();
    double[] vectorLogs= new double[104];
    private int topologyNumber;

    // TODO: Currently only single sink topology generation is supported
    private final Topology sinkTopology;

    public double[] getVectorLogs() {
        return vectorLogs;
    }

    public void setVectorLogs(double[] vectorLogs) {
        this.vectorLogs = vectorLogs;
    }


    public Topology getSinkTopology() {
        return sinkTopology;
    }

    //private int nodeNumber = sinkTopologies.getNodeNumber()

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

    /**
     * prepare vector logs to be used for learning the cost model
     * Each below operators will be encoded into 7 variables so overall 98 for 14 operators: Plat1, Plat2, Top1, Top2, Top3, Top4, Select
     * in this order "map", "filter", "flatmap", "reduce", "globalreduce", "distinct",
     "groupby","sort","join", "union", "cartesian","repeat","collectionsource", "collect"

     and added also: Input cardinality, DataQuantaSize at the end and numberPipeline; numberJunture; numberLoop; numberDuplicate at the beginning
     TODO: to be added datamovement and Topology encoding
     * @return
     */

    public void prepareVectorLogs(){
        double[] logs = new double[105];
        // Loop through all subShapes
        this.subShapes.stream()
                .forEach(s->{
                    logs[0]=s.getPipelineTopologies().size();
                    logs[1]=s.getJunctureTopologies().size();
                    logs[2]=s.getLoopTopologies().size();
                    logs[3]=0;
                    // Loop through all topologies
                    s.allTopologies.stream()
                            .forEach(t -> {
                                // Loop through all nodes
                                t.getNodes().stream()
                                        .forEach(tuple ->{
                                            int start = 4;
                                            switch (tuple.getField0()){
                                                case "map":
                                                    fillLog(tuple,logs,t,start);
                                                    break;
                                                case "filter":
                                                    fillLog(tuple,logs,t,start +7);
                                                    break;
                                                case "flatmap":
                                                    fillLog(tuple,logs,t,start +14);
                                                    break;
                                                case "reduce":
                                                    fillLog(tuple,logs,t,start +21);
                                                    break;
                                                case "globalreduce":
                                                    fillLog(tuple,logs,t,start +28);
                                                    break;
                                                case "distinct":
                                                    fillLog(tuple,logs,t,start +35);
                                                    break;
                                                case "groupby":
                                                    fillLog(tuple,logs,t,start +42);
                                                    break;
                                                case "sort":
                                                    fillLog(tuple,logs,t,start +49);
                                                    break;
                                                case "join":
                                                    fillLog(tuple,logs,t,start +56);
                                                    break;
                                                case "union":
                                                    fillLog(tuple,logs,t,start +63);
                                                    break;
                                                case "cartesian":
                                                    fillLog(tuple,logs,t,start +70);
                                                    break;
                                                //case "repeat":
                                                //    fillLog(tuple,logs,t,start +77);
                                                //    break;
                                                case "repeat":
                                                    fillLog(tuple,logs,t,start +77);
                                                    break;
                                                case "textsource":
                                                    fillLog(tuple,logs,t,start +84);
                                                    break;
                                                case "callbacksink":
                                                    fillLog(tuple,logs,t,start +91);
                                                    break;
                                            }
                                        });
                                });
                    averageSelectivityComplexity(logs);
                    s.setVectorLogs(logs.clone());
                    // reinitialize log array every subShape
                    Arrays.fill(logs, 0);
                });
    }

    void averageSelectivityComplexity(double[] logs){
        for(int i=9; i<=97; i=i+7){
            if(logs[i-4]+logs[i-5]!=0){
                logs[i]=logs[i]/(logs[i-4]+logs[i-5]);
                logs[i+1]=logs[i+1]/(logs[i-4]+logs[i-5]);
            }
        }
    }

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

        if (t.isPipeline())
            logs[start+2]+=1;
        else if(t.isJuncture())
            logs[start+3]+=1;
        else if(t.isLoop())
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
            } else{
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
            // chack if there's an output node connected to

        }
    }

    /**
     * Create a string to describe the shape; for printing purpose
     * @return
     */
    public String toString(){
        return"";
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
}
