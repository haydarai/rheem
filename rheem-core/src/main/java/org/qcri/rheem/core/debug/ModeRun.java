package org.qcri.rheem.core.debug;

/**
 * Created by bertty on 10-05-17.
 */
public class ModeRun {
    private static ModeRun NORMAL_MODE;

    static {
        NORMAL_MODE = new ModeRun();
    }

    private TypeRunMode mode;

    private TypeStatusProcess process;

    public ModeRun(){
        this.process = TypeStatusProcess.CONTINUE;
        this.mode    = TypeRunMode.NORMAL;
    }

    public void setMode(TypeRunMode mode){
        this.mode = mode;
    }

    public TypeRunMode getMode(){
        return this.mode;
    }

    public boolean isDebugMode(){
        return this.mode.isDebugMode();
    }

    public boolean isNormalMode(){
        return this.mode.isNormalMode();
    }

    public void pauseProcess(){
        this.process = TypeStatusProcess.PAUSE;
    }

    public void stopProcess(){
        this.process = TypeStatusProcess.STOP;
    }

    public void continueProcess(){
        this.process = TypeStatusProcess.CONTINUE;
    }

    private TypeStatusProcess getProcess(){
        return this.process;
    }

    public boolean isPauseProcess(){
        return this.process.isPaused();
    }

    public boolean isStopProcess(){
        return this.process.isStoped();
    }

    public boolean isContinueProcess(){
        return this.process.isContinues();
    }

    public static ModeRun normalInstance(){
        return NORMAL_MODE;
    }
}
