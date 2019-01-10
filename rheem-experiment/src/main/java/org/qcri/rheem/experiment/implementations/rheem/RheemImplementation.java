package org.qcri.rheem.experiment.implementations.rheem;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.enviroment.RheemEnviroment;
import org.qcri.rheem.experiment.wordcount.WordCountRheemImplementation;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.ArrayList;
import java.util.List;

public abstract class RheemImplementation extends Implementation {

    protected List<Operator> sinks = new ArrayList<>();
    public RheemImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }


    @Override
    protected void preExecutePlan() {
        System.out.println("Rheem Implementation");
    }

    @Override
    protected RheemResults postExecutePlan() {
        RheemPlan plan = new RheemPlan(sinks.toArray(new Operator[0]));

        RheemContext rheemContext = ((RheemEnviroment)this.enviroment).getEnviroment();

        rheemContext.execute(plan, ReflectionUtils.getDeclaringJar(WordCountRheemImplementation.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

        return this.results;
    }
}
