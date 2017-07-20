package org.qcri.rheem.profiler.core.api;

import org.apache.pig.builtin.TOP;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.profiler.util.RrdAccessor;
import org.qcri.rheem.spark.Spark;
import org.rrd4j.ConsolFun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.SHA;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Contains the {@link OperatorProfiler}s that respects an instantiated {@link Topology} that will be used to build the {@link RheemPlan}
 * in the {@link org.qcri.rheem.profiler.core.ProfilingRunner};
 *
 * Describes a Profiling plan that will be used in the runner to create a rheem plan
 */
public class PlanProfiler {

    public OperatorProfiler sourceOperatorProfiler;

    public List<OperatorProfiler> unaryOperatorProfilers;

    public List<OperatorProfiler> binaryOperatorProfilers;

    /**
     * Sink operator profiler
     */
    public OperatorProfiler sinkOperatorProfiler;

    private Shape shape;

    private static ProfilingConfig profilingConfig;

    private static RheemContext rheemContext;


    public PlanProfiler(Shape shape, ProfilingConfig profilingConfig) {
        this.shape = shape;
        this.profilingConfig = profilingConfig;
    }


    /**
     * Builds a Rheem plan
     * @return
     */
    /*
    private PlanProfiler buildPlan(List<OperatorProfiler> operatorProfilers){
        //assert this.topology.get
        for(OperatorProfiler operatorProfiler:operatorProfilers){

        }
    }*/


    public OperatorProfiler getSourceOperatorProfiler() {
        return sourceOperatorProfiler;
    }

    public void setSourceOperatorProfiler(OperatorProfiler sourceOperatorProfiler) {
        this.sourceOperatorProfiler = sourceOperatorProfiler;
    }

    public List<OperatorProfiler> getUnaryOperatorProfilers() {
        return unaryOperatorProfilers;
    }

    public void setUnaryOperatorProfilers(List<OperatorProfiler> unaryOperatorProfilers) {
        this.unaryOperatorProfilers = unaryOperatorProfilers;
    }

    public OperatorProfiler getSinkOperatorProfiler() {
        return sinkOperatorProfiler;
    }

    public void setSinkOperatorProfiler(OperatorProfiler sinkOperatorProfiler) {
        this.sinkOperatorProfiler = sinkOperatorProfiler;
    }

}
