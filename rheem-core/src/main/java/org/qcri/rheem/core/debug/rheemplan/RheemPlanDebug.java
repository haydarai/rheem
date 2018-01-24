package org.qcri.rheem.core.debug.rheemplan;

import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.Executor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bertty on 16-05-17.
 */
public class RheemPlanDebug extends RheemPlan {



    private static List<ExecutionPlan> executionPlans = new ArrayList<>();

    private static List<CrossPlatformExecutor> crossPlatformExecutors = new ArrayList<>();

    private static List<Executor> executors = new ArrayList<>();


    private static int plansExecuted = 0;


    /**
     * Creates a new instance and does some preprocessing (such as loop isolation).
     *
     * @param sinks the sinks of the new instance
     */
    public RheemPlanDebug(Operator... sinks) {
        super(sinks);
        //ModeRun.getInstance().setModeDebug();
    }

    public void addExecutionPlan(ExecutionPlan executionPlan) {
        executionPlans.add( executionPlan );
    }

    public void addCrossPlatformExecutor(CrossPlatformExecutor crossPlatformExecutor){
        crossPlatformExecutors.add(crossPlatformExecutor);
    }

    public void reduccionStages(){
        int size = crossPlatformExecutors.size();
        if( size <= 1){
            return;
        }
        CrossPlatformExecutor previus = crossPlatformExecutors.get(size - 2);
        CrossPlatformExecutor current = crossPlatformExecutors.get(size - 1);


      //  ExecutionStage

    }

    public void addPlansExecuted(){
        plansExecuted++;
    }

    public static boolean stageIsReady(ExecutionStage stage){
       /* CrossPlatformExecutor previus = crossPlatformExecutors.get(plansExecuted);
        Set<ExecutionStage> finished  = previus.getCompletedStages();
        for(ExecutionStage stageFinished:finished){
            if(stage.isSimilar(stageFinished)){
                return true;
            }
        }*/
        return false;
    }

    public static void addExecutor(Executor executor){
        executors.add(executor);
    }

    public static Executor getLastExecutor(Class type){
        ArrayList<Executor> element = new ArrayList();
        for(Executor executor: executors){
            if( type.isInstance(executor)){
                element.add(executor);
            }
        }
        if(element.isEmpty()){
            return null;
        }
        return element.get(element.size()-1);
    }
}
