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
}
