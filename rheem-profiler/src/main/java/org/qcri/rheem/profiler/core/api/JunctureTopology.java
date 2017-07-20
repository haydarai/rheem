package org.qcri.rheem.profiler.core.api;

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

    public JunctureTopology(int nodeNumber){
        this.inputTopologySlots = new InputTopologySlot[2];
        this.outputTopologySlots = new OutputTopologySlot[1];

        this.inputTopologySlots[0] = new InputTopologySlot<>("in0", this);
        this.inputTopologySlots[1] = new InputTopologySlot<>("in1", this);
        this.outputTopologySlots[0] = new OutputTopologySlot<>("out", this);
        this.nodeNumber = nodeNumber;
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
    public Topology createCopy(){
        InputTopologySlot[] tmpInputTopologySlots = new InputTopologySlot[2];
        OutputTopologySlot[] tmpOutTopologySlots = new OutputTopologySlot[1];


        //for(InputTopologySlot in:this.inputTopologySlots){
        tmpInputTopologySlots[0]=this.inputTopologySlots[0].clone();
        tmpInputTopologySlots[1]=this.inputTopologySlots[1].clone();

        //}

        for(OutputTopologySlot out:this.outputTopologySlots){
            tmpOutTopologySlots[0]=out.clone();
        }

        JunctureTopology newTopology = new JunctureTopology();

        newTopology.setInputTopologySlots(tmpInputTopologySlots);

        newTopology.setOutputTopologySlots(tmpOutTopologySlots);

        return newTopology;
    }

    /**
     * Connects the pipeline topology to another successive {@link Topology}
     */
    //static public void connectTo(int thisOutputIndex, PipelineTopology that0, PipelineTopology that1, int thatInputIndex0,int thatInputIndex1){
    //    that0.connect
    //}
}
