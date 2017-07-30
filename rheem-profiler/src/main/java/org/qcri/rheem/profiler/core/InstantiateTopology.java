package org.qcri.rheem.profiler.core;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.profiler.core.api.OperatorProfiler;
import org.qcri.rheem.profiler.core.api.PipelineTopology;
import org.qcri.rheem.profiler.core.api.Topology;
import org.qcri.rheem.profiler.core.api.TopologyInstance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

/**
 * Instantiate a topology; filling the topology with nodes to be replaced with execution operators;
 * The filling starts with the {@link Topology} connected with the sink then fills the connected {@link Topology}s with associated number
 * of nodes
 */
public class InstantiateTopology {



    // is a structure designed to contains the topology after that has been filled with nodes;
    // PS: Currently is not fully being used
    private List<TopologyInstance> topologyInstances;

    // Number of total node number
    private static int totalNodeNumber;

    // Number of node to fill in each pipeline topology
    private static int equalFillNumber;

    // the rest number of node to fill with the topologies; the below rest will be used to generate different instances with
    // exhaustively filling all possible combinations of the topologies
    private static int restFillNumber;

    //
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
        for(Topology sinkTopology:sinkTopologies){
            // first fill with equal number nodes ie each pipeline topology
            int PipelineNumber = sinkTopology.getNodeNumber();

            equalFillNumber = totalNodeNumber/PipelineNumber;
            restFillNumber =totalNodeNumber%PipelineNumber;
            instantiateTopology(sinkTopology);
        }
        TopologyInstance topologyInstance = new TopologyInstance();
        return topologyInstance;
    }

    /**
     * Instantiate the topology with the number of nodes it contains BUT the filling happens in the Profiling Plan Builder
     * @param topology
     * @return
     */

    public static TopologyInstance instantiateTopology(Topology topology){

        //Topology tmpTopology = sinkTopology;
        // Stop when we reach the source topologies from the
        //do {
            // Handle the case if the sink is pipeline Topology
        Stack<Tuple2<String, OperatorProfiler>> nodes = new Stack<>();

        if (topology.isPipeline()){
                // here we try to fill the topologies as we go up until sources with actual nodes
                // update node number of current topology



                //Set nodes for the sink topology

                // TODO: add the exhaustively filling using the restFillingNodes all possible combinations of the topologies
                for(int i=1;i<=equalFillNumber;i++)
                    nodes.push(new Tuple2<>("unaryNode", new OperatorProfiler() {}));
                // Add one node from the restFillingNodes
                if (restFillNumber!=0) {
                    nodes.push(new Tuple2<String, OperatorProfiler>("unaryNode", new OperatorProfiler(){}));
                    restFillNumber -= 1;
                }
                 /*   // set node number of the current topology
                    if (topology.getNodeNumber()==-1){
                        topology.setNodeNumber(equalFillNumber+1);
                    }
                } else {
                    // set node number of the current topology
                    if (topology.getNodeNumber()==-1){
                        topology.setNodeNumber(equalFillNumber);
                    }
                }*/

                // correct node number
                topology.setNodeNumber(nodes.size());
                topology.setNodes(nodes);

            } else {
                // Handle the case of Juncture topology

                //Set nodes for the sink topology
                //LinkedHashMap nodes = new LinkedHashMap();
                // Always set only one node number that will be replaced in the ProfilingPlanBuilder with a binary Profiling Operator
                nodes.push(new Tuple2<>("binaryNode", new OperatorProfiler() {}));

                topology.setNodes(nodes);

            }
            // Handle the case if the sink is juncture Topology


        // recurse the predecessor tpgs
        //get the predecessors of tmp topology
        if  (!(topology.getInput(0).getOccupant()==null)){
            List<Topology> predecessors = topology.getPredecessors();
            for(Topology t:predecessors)
                instantiateTopology(t);
        }

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

        Stack<Tuple2<String, OperatorProfiler>> nodes = new Stack<>();
        // Check if the current pipeline topology need a source node
        if (pipelineTopology.getAllInputs().length==0)
            nodes.push(new Tuple2<String, OperatorProfiler>("sourceNode", new OperatorProfiler(){}));

        for(int i=1;i<=pipelineTopology.getNodeNumber();i++)
            nodes.push(new Tuple2<String, OperatorProfiler>("unaryNode", new OperatorProfiler(){}));
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
