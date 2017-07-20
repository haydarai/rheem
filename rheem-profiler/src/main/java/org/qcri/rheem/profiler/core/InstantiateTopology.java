package org.qcri.rheem.profiler.core;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.profiler.core.api.OperatorProfiler;
import org.qcri.rheem.profiler.core.api.PipelineTopology;
import org.qcri.rheem.profiler.core.api.Topology;
import org.qcri.rheem.profiler.core.api.TopologyInstance;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Instantiate a topology; filling the topology with nodes to be replaced with execution operators;
 * The filling starts with the {@link Topology} connected with the sink then fills the connected {@link Topology}s with associated number
 * of nodes
 */
public class InstantiateTopology {



    private List<TopologyInstance> topologyInstances;

    private static int totalNodeNumber;

    public InstantiateTopology(List<Topology> sinkTopologies) {
        this.topologyInstances = topologyInstances;
    }

    /**
     * Method that instantiates all generated Topologies
     * @param sinkTopologies
     * @return
     */
    public static TopologyInstance instantiateTopology(List<Topology> sinkTopologies, int nodeNumber){
        totalNodeNumber = nodeNumber;
        for(Topology sinkTopology:sinkTopologies)
            instantiateTopology(sinkTopology);
        TopologyInstance topologyInstance = new TopologyInstance();
        return topologyInstance;
    }

    /**
     * Instantiate the topology
     * @param sinkTopology
     * @return
     */

    public static TopologyInstance instantiateTopology(Topology sinkTopology){

        Topology tmpTopology = sinkTopology;
        // Stop when we reach the source topologies from the
        //do {
            // Handle the case if the sink is pipeline Topology
            if (sinkTopology.isPipeline()){
                // here we try to fill the topologies as we go up until sources with actual nodes
                int PipelineNumber = sinkTopology.getNodeNumber();
                // update node number of current topology

                // first fill with equal number nodes ie each pipeline topology
                int equalFillNumber = totalNodeNumber/PipelineNumber;
                int restNumber =totalNodeNumber%PipelineNumber;

                //Set nodes for the sink topology
                LinkedHashMap nodes = new LinkedHashMap();

                for(int i=1;i<=equalFillNumber;i++)
                    nodes.put(i, new Tuple2<String, OperatorProfiler>("unaryNode", new OperatorProfiler() {}));

                tmpTopology.setNodes(nodes);

                // get the predecessor of tmp topology
               if  (!(sinkTopology.getInput(0).getOccupant()==null)){
                   List<Topology> predecessors = tmpTopology.getPredecessors();
                   for(Topology t:predecessors)
                       instantiateTopology(t);
               }
                // recurse the predecessor tpg
            } else {
                // Handle the case of Juncture topology

                //Set nodes for the sink topology
                LinkedHashMap nodes = new LinkedHashMap();
                // Always set only one node number that will be replaced in the ProfilingPlanBuilder with a binary Profiling Operator
                nodes.put(1, new Tuple2<String, OperatorProfiler>("binaryNode", new OperatorProfiler() {}));

                //get the predecessors of tmp topology
                if  (!(sinkTopology.getInput(0).getOccupant()==null)){
                    List<Topology> predecessors = tmpTopology.getPredecessors();
                    for(Topology t:predecessors)
                        instantiateTopology(t);
                }
                // recurse the predecessor tpgs
            }
            // Handle the case if the sink is juncture Topology



            // loop for source topologies
        //} while (sinkTopology.getAllInputs().length!=0);

        TopologyInstance topologyInstance = new TopologyInstance();
        return topologyInstance;
    }
    /**
     * Instantiate a pipeline Topology
     * @param pipelineTopology
     * @return
     */
    private void InstantiatePipelinetopology(PipelineTopology pipelineTopology){

        LinkedHashMap<Integer,String> nodes = new LinkedHashMap<>();
        // Check if the current pipeline topology need a source node
        if (pipelineTopology.getAllInputs().length==0)
            nodes.put(0,"source");

        for(int i=1;i<=pipelineTopology.getNodeNumber();i++)
            nodes.put(i, "unaryNode");

        pipelineTopology.setNodes(nodes);
        //Topology topology = new Topology();
        //topology.setNodeNumber(nodesNumber);
        //topology.setNodes(nodes);



        //return new TopologyInstance();
    }

    private static void InstantiatePipelinetopology(int nodesNumber){
        TopologyInstance nodes = new TopologyInstance();
        for(int i=0;i<nodesNumber;i++)
            nodes.add(i, "node"+i);
        //Topology topology = new Topology();
        //topology.setNodeNumber(nodesNumber);
        //topology.setNodes(nodes);
        //return new TopologyInstance();
    }
}
