package org.qcri.rheem.experiment.utils.results.type;


import java.net.URI;

public class FileResult implements RheemResult {

    private String path;

    public FileResult(String path) {
        this.path = path;
    }

    public String getPath(){
        return this.path;
    }

    public URI getURI(){
        return URI.create(this.getPath());
    }
}
