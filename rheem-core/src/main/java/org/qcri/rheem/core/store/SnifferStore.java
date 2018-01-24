package org.qcri.rheem.core.store;


import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.SnifferBase;
import org.qcri.rheem.core.util.MultiMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SnifferStore extends OperatorStore<SnifferBase, Operator>{

    private Collection<Operator> sink_operators;
    //previus construct
    {
        this.execution_operators    = new ArrayList<>();
       // this.rheem_operators        = new ArrayList<>();
        this.large_conexion         = new MultiMap<>();
        this.inverse_large_conexion = new MultiMap<>();
        this.sink_operators         = new ArrayList<>();
    }

    public SnifferStore(){}


    public void addConexion(Operator sink, SnifferBase sniffer){
        this.large_conexion.putSingle(sniffer, sink);
        this.inverse_large_conexion.putSingle(sink, sniffer);
        if(! this.execution_operators.contains(sniffer)){
            this.execution_operators.add(sniffer);
        }
        if( ! this.sink_operators.contains(sink) ){
            this.sink_operators.add(sink);
        }
    }

    /** TODO: posible codigo no ocupado, ver para eliminar
    public void showConexion(){
        Set<Map.Entry<SnifferBase, Set<Operator>>> entries = this.large_conexion.entrySet();
        System.out.println("sniffer --+ sinks");
        for(Map.Entry<SnifferBase, Set<Operator>> tmp: entries){
            System.out.println("Sniffer: "+tmp.getKey().getName());
            for(Operator sink: tmp.getValue()){
                System.out.println("   sink:  "+sink.getName()+" exec: "+sink.isExecutionOperator());
            }
            System.out.println();
        }
        System.out.println();
        System.out.println();

        Set<Map.Entry<Operator, Set<SnifferBase>>> entries2 = this.inverse_large_conexion.entrySet();
        System.out.println("sniffer +-- sinks");
        for(Map.Entry<Operator, Set<SnifferBase>> tmp: entries2){
            System.out.println("Sink: "+tmp.getKey().getName());
            for(SnifferBase sniffer: tmp.getValue()){
                System.out.println("   sniffer:  "+sniffer.getName()+" exec: "+sniffer.isExecutionOperator());
            }
            System.out.println();
        }
        System.out.println();
        System.out.println();
    }
     */

    public boolean sinkContains(Operator sink){
        return this.sink_operators.contains(sink);
    }

    public void killConexion(Operator op){
        Set<SnifferBase> sniffers = this.inverse_large_conexion.get(op);

        for(SnifferBase sniffer: sniffers){
            Set<Operator> sinks = this.large_conexion.get(sniffer);
            if(sinks.size() == 1){
                sniffer.selfKill();
            }

        }
    }
}
