package org.qcri.rheem.core.debug;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.store.SnifferStore;

import java.util.*;

/**
 * Created by bertty on 20-11-17.
 */
public class SnifferMakerConexion {

    private Collection<ExecutionStage> stages;

    private Collection<Operator> sniffers;

    private Operator actualSink;

    private SnifferStore store = new SnifferStore();



    public SnifferMakerConexion(Collection<ExecutionStage> stages){
        this.sniffers = new ArrayList<>();
        this.stages = stages;
    }

    public void makeConexion(){
        int order = 1;
        for(ExecutionStage stage: stages){
            Collection<ExecutionTask> allTask = stage.getAllTasks();

            for(ExecutionTask task: stage.getTerminalTasks()){
                travelToTop(task, stage, true);
            }
        }
    }

    private void travelToTop(ExecutionTask task, ExecutionStage stage, boolean terminal){
        Channel[] inputs = task.getInputChannels();
        int size = inputs.length;
        if(!stage.getAllTasks().contains(task)){
            return;
        }

        visit(task.getOperator(), terminal);
        for( int i = 0; i < size; i++ ){
            ExecutionTask     previus_task     = inputs[i].getProducer();
            ExecutionOperator previus_operator = previus_task.getOperator();
            if(previus_operator.isSniffer()){
                Channel channel_1 = previus_task.getOutputChannel(1);
                if(channel_1 == inputs[i]){
                    break;
                }
            }
            travelToTop(inputs[i].getProducer(), stage, false);
        }
    }

    private void visit(Operator op, boolean terminal ){
        if(terminal){
            this.actualSink = op;
        }
        if(op.isSniffer()){
            if( ! this.sniffers.contains(op)){
                this.sniffers.add(op);
            }
            this.store.addConexion(this.actualSink, (SnifferBase) op);
        }
    }

    public SnifferStore getSnifferStore(){
        return this.store;
    }
}
