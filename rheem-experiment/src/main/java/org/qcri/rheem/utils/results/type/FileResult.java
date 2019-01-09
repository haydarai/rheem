package org.qcri.rheem.utils.results.type;


public class FileResult implements RheemResult {

    private String path;

    public FileResult(String path) {
        this.path = path;
    }

    public String getPath(){
        return this.path;
    }
}
