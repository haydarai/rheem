package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Created by migiwara on 07/07/17.
 */
public class PipelineTopology extends TopologyBase implements Topology {

    /**
     * Input topology slot for the {@link PipelineTopology}
     */
    //static private InputTopologySlot[] inputTopologySlots;

    /**
     * Output topology slot for the {@link PipelineTopology}
     */
    //static private OutputTopologySlot[] outputTopologySlots;

    /**
     * Number of Nodes inside the {@link PipelineTopology}
     */
    //private int nodeNumber;

    public PipelineTopology(){
        this.inputTopologySlots = new InputTopologySlot[1];
        this.outputTopologySlots = new OutputTopologySlot[1];
        this.inputTopologySlots[0] = new InputTopologySlot("in", this);
        this.outputTopologySlots[0] = new OutputTopologySlot("out", this);
    }

    public PipelineTopology(int nodeNumber){

        this.inputTopologySlots = new InputTopologySlot[1];
        this.outputTopologySlots = new OutputTopologySlot[1];
        this.inputTopologySlots[0] = new InputTopologySlot("in", this);
        this.outputTopologySlots[0] = new OutputTopologySlot("out", this);
        this.nodeNumber=nodeNumber;
    }

    public PipelineTopology(InputTopologySlot inputTopology, OutputTopologySlot outputTopology, int nodeNumber) {
        this.inputTopologySlots[0] = new InputTopologySlot<>("in", this);
        this.outputTopologySlots[0] = new OutputTopologySlot<>("out", this);
        this.nodeNumber = nodeNumber;
    }

    /**
     * Connects the pipeline topology to another successive {@link Topology}
     */
    public void connectTo(int thisOutputIndex, Topology that, int thatInputIndex){
        // create an input slot for the topology to connect To
        // that.setInput(thatInputIndex,new InputTopologySlot<>("in", that));
        final InputTopologySlot inputSlot = that.getInput(thatInputIndex);
        // create output slot for current Topology
        // outputTopologySlots[0] = new OutputTopologySlot<>("out", this);

        // reget the created output slot of the current Topology
        final OutputTopologySlot outputSlot = this.getOutput(thisOutputIndex);

        // connect this output slot to that input slot
        outputSlot.connectTo(inputSlot);
    }

    /**
     * create a copy of current topology
     * @return
     */
    public Topology createCopy(){
        PipelineTopology newTopology = new PipelineTopology();

        InputTopologySlot[] tmpInputTopologySlots = new InputTopologySlot[1];
        OutputTopologySlot[] tmpOutTopologySlots = new OutputTopologySlot[1];


        for(InputTopologySlot in:this.inputTopologySlots){
            tmpInputTopologySlots[0]=in.clone();
        }

        for(OutputTopologySlot out:this.outputTopologySlots){
            tmpOutTopologySlots[0]=out.clone();
        }

        //InputTopologySlot[] inputTopologySlots = (InputTopologySlot[]) Arrays.stream(this.inputTopologySlots).map(el->el.clone())
        //        .collect(Collectors.toList()).toArray();

        //OutputTopologySlot[] outputTopologySlots = (OutputTopologySlot[]) Arrays.stream(this.outputTopologySlots).map(el->el.clone())
        //        .collect(Collectors.toList()).toArray();

        newTopology.setInputTopologySlots(tmpInputTopologySlots);

        //newTopology.setOutputTopologySlots(tmpOutTopologySlots);

        return newTopology;
    }
}
