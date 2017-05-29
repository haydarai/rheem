package org.qcri.rheem.spark.debug;

import org.qcri.rheem.spark.execution.SparkExecutor;

/**
 * Created by bertty on 23-05-17.
 */
public class SparkDebug {

    public static boolean killSpark(){
        SparkExecutor se = (SparkExecutor) null;//RheemPlanDebug.getLastExecutor(SparkExecutor.class);
        if(se == null){
            return false;
        }
        se.sc.cancelAllJobs();
        System.out.println("dahjdhaskjhdjkahdkjahsdjkhaskjdlha aahsjkdhaskjdhkajslhdlkjashdkljashdlkahsldkjhaskljdhaslkdhaskjldhaksjldhalkjs");
        return true;
    }
}
