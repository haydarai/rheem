package org.qcri.rheem.tests.benchmark;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

public class BaseBenchmark extends Benchmark{
    @Override
    protected RheemPlan doExecute() {
        this.connect("source", "cut");
        this.connect("cut", "labels");
        this.connect("labels", "reduce");
        this.connect("reduce", "sink");

        return new RheemPlan(this.operators.get("sink"));
    }
}
