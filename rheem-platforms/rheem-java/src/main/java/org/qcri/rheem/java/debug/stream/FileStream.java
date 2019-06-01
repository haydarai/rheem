package org.qcri.rheem.java.debug.stream;

//import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.debug.collection.RheemList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bertty on 16-05-17.
 */
public class FileStream extends StreamRheem{

    private ModeRun          modeRun;

    private String           path;
    //  private RandomAccessFile randomAccessFile;
    private String           lineCurrent;
    private List<String>     memory;
    private Iterator<String> iterator;
    private BufferedReader   reader;
    private int              index;
    private boolean          readFile;
            boolean flag_test;



    public FileStream(TextFileSource op, ModeRun mode) {
        super();
        this.path = op.getInputUrl().trim();
        setElements();
        this.modeRun = mode;
    }

    /*public FileStream(JSONSource op, ModeRun mode) {
        super();
        this.path = op.getInputUrl().trim();
        setElements();
        this.modeRun = mode;
    }*/

    private void setElements(){
        this.reader   = openFile();
        this.memory   = new RheemList<>();
        this.readFile = true;
        this.index    = 0;
    }


    private BufferedReader openFile(){
        try {
            FileSystem fs = FileSystems.getFileSystem(this.path).orElseThrow(
                    () -> new RheemException(String.format("Cannot access file system of %s.", this.path))
            );
            return new BufferedReader(new InputStreamReader(fs.open(this.path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void closeFile(){
        try {
            this.reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    @Override
    public boolean hasNext() {
        try {
            while(this.modeRun.isPauseProcess()){
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            this.modeRun.stopProcess();
        }

        if(this.modeRun.isStopProcess()){
            loadMemory();
            return false;
        }

        if( this.readFile ){
            return this.readFile();
        }else{
            return this.readMemory();
        }
    }

    private boolean readFile(){
        try {
            if( ! this.reader.ready() ){
                loadMemory();
                return false;
            }

            this.lineCurrent = this.reader.readLine();

            if( this.lineCurrent != null ){
                this.memory.add(this.lineCurrent);
                return true;
            }else{
                this.closeFile();
                return false;
            }
        } catch (IOException e) {
            return false;
        }
    }

    private void loadMemory(){
        this.readFile = false;
        this.iterator = this.memory.iterator();
        this.flag_test = true;
    }

    private boolean readMemory(){
        try {
            if(this.flag_test){
                this.flag_test = false;
            }
            if (!this.iterator.hasNext()) {
                if (this.reader.ready()) {
                    this.readFile = true;
                    return hasNext();
                }
                loadMemory();
                return false;
            }

            this.lineCurrent = this.iterator.next();
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    @Override
    public Object next() {
        return this.lineCurrent;
    }

}
