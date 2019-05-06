package org.qcri.rheem.tests.benchmark;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

public class SinkEmptyBenchmark extends Benchmark{

    @Override
    protected RheemContext preExecute() {
        return getDebug(super.preExecute());
    }

    @Override
    protected RheemPlan doExecute() {
        this.connect("source", "cut");
        this.connect("cut", "labels");
        this.connect("labels", "sniffer");
        this.connect("sniffer", "reduce");
        this.connect("sniffer", 1, "sink_empty", 0);
        this.connect("reduce", "sink");

        return new RheemPlan(
            this.operators.get("sink"),
            this.operators.get("sink_empty")
        );
    }
}
