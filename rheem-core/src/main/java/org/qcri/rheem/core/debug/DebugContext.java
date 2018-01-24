package org.qcri.rheem.core.debug;

import org.qcri.rheem.core.debug.repository.Repository;

import java.util.*;

public class DebugContext {

    private ModeRun modeRun;

    private Map<String, Iterator> inputsIterators = new HashMap<>();

    private  Map<String, Repository> repositories = new HashMap<>();

    public DebugContext(ModeRun modeRun){
        this.modeRun = modeRun;
    }


    public void addInput(String name, Iterator iter){
        this.inputsIterators.put(name, iter);
    }


    public Iterator getInput(String name){
        return this.inputsIterators.get(name);
    }

    public void addRepository(String name, Repository repository){
        this.repositories.put(name, repository);
    }

    public Repository getRepository(String name){
        return this.repositories.get(name);
    }

    public Iterator getIntermediateInput(){
        return null;
    }


    public ModeRun getModeRun(){
        return this.modeRun;
    }
















    public void pauseProcess(){
        this.modeRun.pauseProcess();
    }
}
