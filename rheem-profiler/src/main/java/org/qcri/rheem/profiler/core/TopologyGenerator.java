package org.qcri.rheem.profiler.core;

import org.qcri.rheem.profiler.core.api.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Generates Topology for profiling plans
 */
public class TopologyGenerator {

    public static List<Topology> getTopologyList() {
        return topologyList;
    }

    /**
     * Generated Topologies
     */
    private static List<Topology> topologyList = new ArrayList<>();

    /**
     * Number of nodes in the topology; is used only to generate topology shapes;
     * It is not used to fill the Topology because that will be the topology instantiate task.
     */
    private static int nodeNumber;

    /**
     * cached node number; is used when generating topologies having more node number than the cached node number;
     *
     */
    private static int cachedNodeNumber = 1;

    /**
     * number of new generted toplogy when {@method generateTopology} is colled;
     *
     */
    private static int newGeneratedTopologies = 0;

    private static ProfilingConfig config;

    /**
     * cached generated topologies; is used when generating topologies having more node number than the cached node number;
     */
    private static List<Topology> cachedTopologyList = new LinkedList<>();


    public TopologyGenerator() {
        nodeNumber = 0;
    }


    public TopologyGenerator( int nodesNumber,  ProfilingConfig configuration) {
        config = configuration;
        nodeNumber = nodesNumber;
    }

    public static void startGeneration(){
        generateTopology(cachedNodeNumber);
    }

    public static List<Topology> generateTopology(int nodesNumber){
        // Disabled case that handles single Operator Topology
        // TODO the below case should be put in an outside part befor the the topology loops
        if((nodesNumber==1)&&(false))
            // Single operator topology
            return singleOperatortopology();

        //List<Topology> topologyList = new LinkedList<>();

        int previousGeneratedTopologies = newGeneratedTopologies;
        newGeneratedTopologies =0;

        for(int i=1;i<=previousGeneratedTopologies;i++){
            Topology tmpPreviousTopology = topologyList.get(topologyList.size()-i).createCopy();

            Topology[] tmp = new Topology[1];

            Topology[] tmp2 = new Topology[1];
            tmp2[0] = topologyList.get(topologyList.size()-i);
            //System.arraycopy(tmp2, 0, tmp, 0,0);
            if (tmpPreviousTopology.isPipeline()){
                // if the tmpPreviousTopology is pipeline the only one topology possiblity to generate (i.e 1 new juncture + 1 new pipeline)
                // Handles the case of creating the first merge Topology (i.e with only one Juncture Topology)
                PipelineTopology tmpPipeline = new PipelineTopology(nodesNumber);
                JunctureTopology tmpJuncture = new JunctureTopology(nodesNumber);

                // Connect the last generated Topology with tmpJuncture
                tmpPreviousTopology.connectTo(0,tmpJuncture,0);
                // Connect the created Pipeline Topology with tmpJuncture
                tmpPipeline.connectTo(0,tmpJuncture,1);

                // Add the new Topology with Juncture
                topologyList.add(tmpJuncture);
                newGeneratedTopologies+=1;

            } else {
                // case of juncture topology; two possible topologies to generate (i.e: first is 1 new pipeline; second: 1 new juncture + 1 new pipeline)

                PipelineTopology tmpPipeline = new PipelineTopology(nodesNumber);
                JunctureTopology tmpJuncture = new JunctureTopology(nodesNumber);

                // Connect the last generated Topology with tmpJuncture
                tmpPreviousTopology.connectTo(0,tmpPipeline,0);

                // Add the first generated topology
                topologyList.add(tmpPipeline);

                PipelineTopology tmpPipeline2 = new PipelineTopology(nodesNumber);
                // Connect the last generated Topology with tmpJuncture
                tmpPreviousTopology.connectTo(0,tmpJuncture,0);
                // Connect the created Pipeline Topology with tmpJuncture
                tmpPipeline2.connectTo(0,tmpJuncture,1);

                // Add the first generated topology
                topologyList.add(tmpJuncture);

                newGeneratedTopologies+=2;
            }
        }

        if(config.isBushyGeneration()){
            // Do bushy generation
            for(int N=2;N<=nodesNumber/2;N++){

                // get list with number of nodes N
                int nodeNumberN = N;
                List<Topology> SubListN = new ArrayList();
                SubListN = topologyList.stream().filter(t -> (t.getNodeNumber()==nodeNumberN))
                        .collect(Collectors.toList());

                // get list with number of nodes M equals to number of current nodes minus N
                final int nodeNumberM = nodesNumber-N;
                List<Topology> SubListM = new ArrayList();
                SubListM = topologyList.stream().filter(t -> (t.getNodeNumber()==nodeNumberM))
                        .collect(Collectors.toList());

                // Do exhaustive merge of both lists
                List<Topology> finalSubListM = SubListM;

                SubListN.stream().forEach(t1 ->{
                    finalSubListM.stream().forEach(t2 -> {
                        Topology newT = mergeTopologies(t1,t2,nodesNumber);
                        topologyList.add(newT);
                        newGeneratedTopologies+=1;
                    });
                });

            }
        }

        // Handles the case of generating One Node Pipeline Topology
        if (nodesNumber==1) {
            topologyList.add(new PipelineTopology(nodesNumber));
            newGeneratedTopologies+=1;
            // exit if the nodeNumber is equal to 1
            if(nodeNumber==1){
                return topologyList;
            } else
                return generateTopology(nodesNumber+1);
        }

        // add recursively topologies until reaching the current TopologyGenerator node number
        //while((nodesNumber+1)<=nodeNumber)
        if ((nodesNumber+1)<=nodeNumber)
            generateTopology(nodesNumber+1);
        return topologyList;
    }

    private static Topology mergeTopologies(Topology t1, Topology t2, int nodeNumber) {
        Topology t1Copy = t1.createCopy();
        Topology t2Copy = t2.createCopy();

        JunctureTopology tmpJuncture = new JunctureTopology(nodeNumber);

        // Merge t1 and t2
        t1Copy.connectTo(0,tmpJuncture,0);
        t2Copy.connectTo(0,tmpJuncture,1);

        return tmpJuncture;
    }

    private static List<Topology> singleOperatortopology(){
        LinkedHashMap<Integer,String> nodes = new LinkedHashMap<>();
        List<Topology> topologyList = new LinkedList<>();
        Topology topology = new TopologyBase();
        //topology.setNodeNumber(1);
        //topology.setNodes(nodes);
        topologyList.add(topology);
        return topologyList;
    }

    private static PipelineTopology Pipelinetopology(int nodesNumber){
        return new PipelineTopology(nodesNumber);
    }
}
