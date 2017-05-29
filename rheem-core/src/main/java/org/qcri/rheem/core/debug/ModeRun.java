package org.qcri.rheem.core.debug;

/**
 * Created by bertty on 10-05-17.
 */
public class ModeRun {
    private static       ModeRun moderun    = null;
    private static final String  DEBUG_MODE = "debug";
    private static final int     NORMAL     = 0;
    private static final int     DEBUG      = 1;

    private static final int     STOP_PROCESS     = 0;
    private static final int     PAUSE_PROCESS    = 1;
    private static final int     CONTINUE_PROCESS = 2;



    private int mode;

    private int process;



    private ModeRun(){
        this.process = CONTINUE_PROCESS;
        this.mode    = NORMAL;
    }

    public static ModeRun getInstance(){
        if(moderun == null){
            moderun = new ModeRun();
        }
        return moderun;
    }

    private void setMode(String mode){
        this.mode = NORMAL;
        if(mode.equalsIgnoreCase(DEBUG_MODE)){
            this.mode = DEBUG;
        }
    }

    private int getMode(){
        return this.mode;
    }

    public static void setModeDebug(){
        getInstance().setMode(DEBUG_MODE);
    }

    public static boolean isDebugMode(){
        return getInstance().getMode() == DEBUG;
    }

    private void _pauseProcess(){
        this.process = PAUSE_PROCESS;
    }

    public static void pauseProcess(){
        getInstance()._pauseProcess();
    }

    private void _stopProcess(){
        this.process = STOP_PROCESS;
    }

    public static void stopProcess(){
        getInstance()._stopProcess();
    }

    private void _continueProcess(){
        this.process = CONTINUE_PROCESS;
    }

    public static void continueProcess(){
        getInstance()._continueProcess();
    }

    private int getProcess(){
        return this.process;
    }

    public static boolean isPauseProcess(){
        return getInstance().getProcess() == PAUSE_PROCESS;
    }

    public static boolean isStopProcess(){
        return getInstance().getProcess() == STOP_PROCESS;
    }

    public static boolean isContinueProcess(){
        return getInstance().getProcess() == CONTINUE_PROCESS;
    }


}
