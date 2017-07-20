package org.qcri.rheem.profiler.core.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by migiwara on 16/07/17.
 */
public class Shape {

    private List<Topology> allTopologies = new ArrayList<>();

    private List<Topology> sourceTopologies = new ArrayList<>();

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

    public int getTopologyNumer() {
        return topologyNumer;
    }

    public Topology getSinkTopology() {
        return sinkTopology;
    }

    // TODO: Currently only single sink topology generation is supported
    private final Topology sinkTopology;

    private List<PipelineTopology> pipelineTopologies = new ArrayList<>();

    private List<JunctureTopology> junctureTopologies = new ArrayList<>();

    private int topologyNumer;

    //private int nodeNumber = sinkTopologies.getNodeNumber()

    public Shape(Topology topology){
        this.sinkTopology = topology;
    }

    public void populateShape(Topology topology){

        // Handle the case if the topology is pipeline Topology
        if (topology.isPipeline()){
            this.pipelineTopologies.add((PipelineTopology) topology);
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
        } else {
            // Handle the case if the topology is juncture Topology
            this.junctureTopologies.add((JunctureTopology) topology);

            //get the predecessors of tmp topology
            if  (!(topology.getInput(0).getOccupant()==null)){
                this.junctureTopologies.add((JunctureTopology) topology);
                List<Topology> predecessors = topology.getPredecessors();
                for(Topology t:predecessors)
                    // recurse for predecessor topologies
                    populateShape(t);
            }else{
                // This case means it's source topology
                sourceTopologies.add(topology);
            }
        }
    }

}
