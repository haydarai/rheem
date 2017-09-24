package org.qcri.rheem.profiler.core.api;

import java.util.Stack;

/**
 * Created by migiwara on 07/07/17.
 */
public class JunctureTopology extends TopologyBase implements Topology {

    /**
     * Input topology slot for the {@link PipelineTopology}
     */
    private InputTopologySlot inputTopology;

    /**
     * Output topology slot for the {@link PipelineTopology}
     */
    private OutputTopologySlot outputTopology;


    public JunctureTopology(){
        this.inputTopologySlots = new InputTopologySlot[2];
        this.outputTopologySlots = new OutputTopologySlot[1];

        this.inputTopologySlots[0] = new InputTopologySlot<>("in0", this);
        this.inputTopologySlots[1] = new InputTopologySlot<>("in1", this);
        this.outputTopologySlots[0] = new OutputTopologySlot<>("out", this);
    }

    public JunctureTopology(int topologyNumber){
        this.inputTopologySlots = new InputTopologySlot[2];
        this.outputTopologySlots = new OutputTopologySlot[1];

        this.inputTopologySlots[0] = new InputTopologySlot<>("in0", this);
        this.inputTopologySlots[1] = new InputTopologySlot<>("in1", this);
        this.outputTopologySlots[0] = new OutputTopologySlot<>("out", this);
        this.topologyNumber = topologyNumber;
    }

    /**
     * Connects the pipeline topology to another successive {@link Topology}
     */
    public void connectTo(int thisOutputIndex, Topology that, int thatInputIndex){
        // create an input slot for the topology to connect To
        //that.setInput(thatInputIndex,new InputTopologySlot<>("in", that));
        final InputTopologySlot inputSlot = that.getInput(thatInputIndex);
        // create output slot for current Topology
        //outputTopologySlots[thisOutputIndex] = new OutputTopologySlot<>("out", this);
        final OutputTopologySlot outputSlot = this.getOutput(thisOutputIndex);

        outputSlot.connectTo(inputSlot);
    }

    /**
     * create a copy of current topology
     * @return
     */
    public Topology createCopy(int topologyNumber){
        InputTopologySlot[] tmpInputTopologySlots = new InputTopologySlot[2];
        OutputTopologySlot[] tmpOutTopologySlots = new OutputTopologySlot[1];


        // new junctureCopy
        JunctureTopology newTopology = new JunctureTopology(topologyNumber);

        //for(InputTopologySlot in:this.inputTopologySlots){
        tmpInputTopologySlots[0]=this.inputTopologySlots[0].clone();

        if (this.inputTopologySlots[0].getOccupant() != null){
            // input1 topology copy
            Topology tmpNewTopology = this.inputTopologySlots[0].getOccupant().getOwner().createCopy(topologyNumber-1);
            // connect the input1Copy topology with the new junctureCopy input1
            tmpInputTopologySlots[0].setOccupant(tmpNewTopology.getOutput(0));
        }

        tmpInputTopologySlots[1]=this.inputTopologySlots[1].clone();

        if (this.inputTopologySlots[1].getOccupant() != null){
            // input2 topology copy
            Topology tmpNewTopology = this.inputTopologySlots[1].getOccupant().getOwner().createCopy(topologyNumber-1);
            // connect the input2Copy topology with the new junctureCopy input2
            tmpInputTopologySlots[1].setOccupant(tmpNewTopology.getOutput(0));
        }

        //}

        for(OutputTopologySlot out:this.outputTopologySlots){
            tmpOutTopologySlots[0] = new OutputTopologySlot("in", newTopology);
            tmpOutTopologySlots[0].setOccupiedSlots(out.getOccupiedSlots());
        }


        newTopology.setInputTopologySlots(tmpInputTopologySlots);

        newTopology.setOutputTopologySlots(tmpOutTopologySlots);

        // Clone the nodes
        newTopology.setNodes((Stack) this.getNodes().clone());

        //Clone the nodenumber
        newTopology.setNodeNumber(this.nodeNumber);
        newTopology.setName(this.getName());

        return newTopology;
    }

    /**
     * Connects the pipeline topology to another successive {@link Topology}
     */
    //static public void connectTo(int thisOutputIndex, PipelineTopology that0, PipelineTopology that1, int thatInputIndex0,int thatInputIndex1){
    //    that0.connect
    //}
}
