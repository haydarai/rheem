package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.basic.data.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

/**
 * Created by migiwara on 08/07/17.
 */
public class TopologyBase implements Topology {


    @Override
    public void setInputTopologySlots(InputTopologySlot[] inputTopologySlots) {
        this.inputTopologySlots = inputTopologySlots;
    }

    @Override
    public void setOutputTopologySlots(OutputTopologySlot[] outputTopologySlots) {
        this.outputTopologySlots = outputTopologySlots;
    }

    /**
     * Input Slots associated with the topology instance
     */
    protected InputTopologySlot[] inputTopologySlots;

    /**
     * Output Slots associated with the topology instance
     */
    protected OutputTopologySlot[] outputTopologySlots;




    /**
     * This important variable saves the the layer of the topology (i.e. for how many number of nodes it was created)
     * PS: first nodeNumber implementation was having this purpose too as well as the number of number of nodes that is
     * updated after instantiation
     */
    protected int topologyNumber;

    /**
     * Number of nodes in the topology
     */
    protected int nodeNumber = -1;

    /**
     * Nodes inside a Topology
     */
    //private LinkedHashMap<Integer,Tuple2<String,OperatorProfiler>> nodes;
    private Stack<Tuple2<String,OperatorProfiler>> nodes = new Stack<>();

    /**
     * Optional name. Helpful for debugging.
     */
    private String name;



    /**
     * true if the topology is a source topology
     *//*
    private boolean isSource = false;

    *//**
     * true if the topology is a sink topology
     *//*
    private boolean isSink = false;*/
    /*
    public Topology(){
        Nodes = new LinkedHashMap();
        nodeNumber = 0;
    }

    public Topology(int nodeNumber, LinkedHashMap nodes) {
        this.nodeNumber = nodeNumber;
        Nodes = nodes;
    }*/

    public int getTopologyNumber() {
        return topologyNumber;
    }

    public int getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(int nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public Stack<Tuple2<String,OperatorProfiler>> getNodes() {
        return this.nodes;
    }

    public void setNodes(Stack nodes) {
        this.nodes = nodes;
    }

    public List<Topology> getPredecessors(){
        InputTopologySlot[] inputSlots = this.getAllInputs();
        List<Topology> predecessors = new ArrayList<>();
        for(InputTopologySlot input:inputSlots){
            OutputTopologySlot output = input.getOccupant();
            predecessors.add(output.getOwner());
        }
        return predecessors;
    }

    /**
     * create a copy of current topology
     * @return
     */
    public Topology createCopy(int topologyNumber){
        Topology newTopology = new TopologyBase();

        newTopology.setInputTopologySlots(this.inputTopologySlots);

        newTopology.setOutputTopologySlots(this.outputTopologySlots);

        return newTopology;
    }

    @Override
    public InputTopologySlot<?>[] getAllInputs() {
        return this.inputTopologySlots;
    }

    @Override
    public OutputTopologySlot<?>[] getAllOutputs() {
        return this.outputTopologySlots;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    /*@Override
    public boolean isSink() {
        return isSink;
    }

    public void setSink(boolean sink) {
        isSink = sink;
    }

    @Override
    public boolean isSource() {
        return isSource;
    }

    public void setSource(boolean source) {
        isSource = source;
    }*/

}
