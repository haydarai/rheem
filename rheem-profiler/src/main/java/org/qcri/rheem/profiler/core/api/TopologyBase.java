package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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
     * Number of nodes in the topology
     */
    protected int nodeNumber = -1;

    /**
     * Nodes inside a Topology
     */
    private LinkedHashMap<Integer,Tuple2<String,OperatorProfiler>> nodes;

    /**
     * Optional name. Helpful for debugging.
     */
    private String name;

    /*
    public Topology(){
        Nodes = new LinkedHashMap();
        nodeNumber = 0;
    }

    public Topology(int nodeNumber, LinkedHashMap nodes) {
        this.nodeNumber = nodeNumber;
        Nodes = nodes;
    }*/

    public int getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(int nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public LinkedHashMap<Integer,Tuple2<String,OperatorProfiler>> getNodes() {
        return this.nodes;
    }

    public void setNodes(LinkedHashMap nodes) {
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
    public Topology createCopy(){
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
}
