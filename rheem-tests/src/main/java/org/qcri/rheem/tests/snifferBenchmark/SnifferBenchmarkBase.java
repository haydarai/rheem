package org.qcri.rheem.tests.snifferBenchmark;

import com.google.api.client.http.HttpRequestFactory;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.basic.plugin.RheemBasic;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.tests.snifferBenchmark.wordcount.WordCountSpecial;

public abstract class SnifferBenchmarkBase {

    private Operator[] normalOperator;
    private Operator[] sniffers;

    public SnifferBenchmarkBase(){}

    public void makeOperators(int n_sniffers){
        this.normalOperator = generateBasePlan();
        this.sniffers = generateSniffers(n_sniffers);
    }

    protected abstract Operator[] generateBasePlan();

    protected Operator[] generateSniffers(int size){
        Operator[] op = new Operator[size];
        for(int i = 0; i < size; i++){
            op[i] = new SnifferOperator<String, Tuple1>(String.class, Tuple1.class).setFunction(
                element -> {
                    return new Tuple1(null);
                }
            );
        }
        return op;
    }

    private RheemPlan generatePlan(){
        Operator current = this.normalOperator[0];
        Operator next = null;
        for(int i = 1; i < this.normalOperator.length; i++){
            boolean clean_next = false;
            if(next != null || i > this.sniffers.length){
                next = this.normalOperator[i];
            }else{
                next = this.sniffers[--i];
            }

            current.connectTo(0, next, 0);
            current = next;

            if(clean_next){
                next = null;
            }
        }
        return new RheemPlan(current);
    }

    public void execute(){
        RheemPlan plan = generatePlan();

        RheemContext context = new RheemContext();
        context.register(Spark.basicPlugin());

        context.execute(
            plan,
            ReflectionUtils.getDeclaringJar(this.getClass()),
            ReflectionUtils.getDeclaringJar(SparkPlatform.class),
            ReflectionUtils.getDeclaringJar(HttpRequestFactory.class),
            ReflectionUtils.getDeclaringJar(com.google.api.client.http.HttpContent.class),
            ReflectionUtils.getDeclaringJar(RheemBasic.class),
            ReflectionUtils.getDeclaringJar(JavaPlatform.class),
            ReflectionUtils.getDeclaringJar(WordCountSpecial.class)
        );
    }






}
