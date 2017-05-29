package org.qcri.rheem.java.debug.stream;

import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.*;

/**
 * Created by bertty on 16-05-17.
 */
public class FileStream extends StreamRheem{

    private String           path;
    private RandomAccessFile randomAccessFile;
    private String           lineCurrent;



    public FileStream(TextFileSource op) {
        super();
        this.path = op.getInputUrl().trim();
        this.randomAccessFile = openFile();
        System.out.println(this.path);
        System.out.println(this.randomAccessFile);
        this.setSize();
    }

    public FileStream(JSONSource op) {
        super();
        this.path = op.getInputUrl().trim();
        this.randomAccessFile = openFile();
        this.setSize();
    }


    protected long getSize(){
        try {
            return this.randomAccessFile.length();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    protected long getCurrent(){
        try{
            return this.randomAccessFile.getFilePointer();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private RandomAccessFile openFile(){
        try {
            FileSystem fs = FileSystems.getFileSystem(this.path).orElseThrow(
                    () -> new RheemException(String.format("Cannot access file system of %s.", this.path))
            );
            return fs.openDebug(this.path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void closeFile(){
        try {
            this.randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    @Override
    public boolean hasNext() {
        try {
            while(ModeRun.isPauseProcess()){
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            ModeRun.stopProcess();
        }


        try {

            if(ModeRun.isStopProcess()){
                this.setFinish(this.randomAccessFile.getFilePointer());
                return false;
            }

            if(!this.lastElementNecessary && this.randomAccessFile.getFilePointer() >= this.finish){
                closeFile();
                return false;
            }

            this.lineCurrent = this.randomAccessFile.readLine();
            if( this.lineCurrent != null ){
                return true;
            }else{
                if(this.randomAccessFile.length() <= this.finish){
                    closeFile();
                    return false;
                }else{
                    this.randomAccessFile.seek(0);
                    this.lastElementNecessary = false;
                    return hasNext();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;

    }

    @Override
    public Object next() {
        return this.lineCurrent;
    }

}
