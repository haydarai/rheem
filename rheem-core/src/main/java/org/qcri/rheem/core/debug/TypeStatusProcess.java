package org.qcri.rheem.core.debug;

/**
 * Created by bertty on 20-11-17.
 */
public enum TypeStatusProcess {
    STOP,
    PAUSE,
    CONTINUE;

    public boolean isStoped(){
        return this == STOP;
    }

    public boolean isPaused(){
        return this == PAUSE;
    }

    public boolean isContinues(){
        return this == CONTINUE;
    }

    public TypeStatusProcess getValue(String original){
        String tmp = original.toLowerCase();
        if(tmp.contains("resume")){
            return TypeStatusProcess.CONTINUE;
        }
        if(tmp.contains("pause")){
            return TypeStatusProcess.PAUSE;
        }
        if(tmp.contains("stop")){
            return TypeStatusProcess.STOP;
        }
        return null;
    }
}
