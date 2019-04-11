package org.qcri.rheem.generator;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.serialize.RheemSerializer;
import org.qcri.rheem.serialize.graph.RheemTraversal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class RheemGenerator {

    private Operator[] operators;
    private RheemPlan plan_seed;
    private org.qcri.rheem.core.plan.rheemplan.Operator[] sinks;

    private Platform[] platforms;

    private Map<String, int[]> configurations_platforms;

    public RheemGenerator(Platform... platforms){
        this.platforms = platforms;
    }

    public RheemGenerator(RheemPlan seed, Platform[] platforms){
        this(platforms);
        this.setRheemPlan(plan_seed);
    }

    public void setRheemPlan(RheemPlan seed){
        this.plan_seed = seed;
        Iterator<Operator> traversal = new RheemTraversal(seed).iterator();

        Collection<Operator> operators = new ArrayList<>();
        Collection<Operator> sinks = new ArrayList<>();

        while(traversal.hasNext()){
            Operator current = traversal.next();
            operators.add(current);
            if(current.isSink()){
                sinks.add(current);
            }
        }

        this.operators = operators.toArray(new Operator[0]);
        this.sinks = sinks.toArray(new Operator[0]);
    }

    public void genExaustive(){
        //TODO we need implement this option
    }

    public void genRandom(int size){
        this.genRandom(size, System.currentTimeMillis());
    }

    public void genRandom(int size, long seed){
        this.configurations_platforms = new HashMap<>(size);
        Random random = new Random(seed);
        int n_operator = this.operators.length;
        for(int i = 0; i < size; ){
            boolean value = genPlatformPicked(n_operator, this.platforms.length, random);
            i += (value)? 1: 0;
            if(!value){
                System.out.println("generating again");
            }
        }

    }

    private boolean genPlatformPicked(int size, int platforms, Random random){
        int[] desicion = new int[size];
        char[] key = new char[size];
        for(int i = 0; i < size; i++){
            desicion[i] = random.nextInt(platforms);
            key[i] = (char)(desicion[i] + '0');
        }
        String key_str = new String(key);
        if(configurations_platforms.containsKey(key_str)){
            return false;
        }else{
            configurations_platforms.put(key_str, desicion);
            return true;
        }
    }

    public void generateAndSave(RheemPlan plan, int size){
        this.setRheemPlan(plan);
        this.generateAndSave(size);
    }

    public void generateAndSave(int size){
        System.out.println("generating configuration of platforms to pick");
        this.genRandom(size);
        System.out.println("configuring the rheem plans");
        Collection<int[]> vectors = this.configurations_platforms.values();
        RheemSerializer serializer = new RheemSerializer();
        for(int[] vector: vectors){
            if(vector.length != this.operators.length){
                throw new RheemException("the vector not correspond with the plan");
            }
            for(int i = 0; i < this.operators.length; i++){
                this.operators[i].clearTargetPlatform();
                this.operators[i].addTargetPlatform(this.platforms[vector[i]]);
            }
            serializer.save(new RheemPlan(this.sinks));
        }
    }

}
