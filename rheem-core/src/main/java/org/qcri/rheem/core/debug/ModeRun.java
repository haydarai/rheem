package org.qcri.rheem.core.debug;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bertty on 10-05-17.
 */
public class ModeRun {
    private static ModeRun NORMAL_MODE;
    private HttpRequestFactory requestFactory;
    private HttpRequest request;
    private int counter;

    static {
        NORMAL_MODE = new ModeRun();
    }

    private TypeRunMode mode;

    private TypeStatusProcess process;

    public ModeRun(){
        this.process = TypeStatusProcess.CONTINUE;
        this.mode    = TypeRunMode.NORMAL;
        try {
            URI uri = URI.create("http://localhost:8080/debug/");
            this.requestFactory = new NetHttpTransport().createRequestFactory();
            this.request = requestFactory.buildGetRequest(new GenericUrl(uri));
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        try {
            counter++;
            if (counter % 100 == 0) {
                String status_current = this.request
                        .execute()
                        .parseAsString();

                this.process = TypeStatusProcess.valueOf(status_current);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
