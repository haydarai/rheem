package org.qcri.rheem.experiment.utils.parameters.type;

import java.net.URI;

public class FileParameter implements RheemParameter {

    private String path;

    public FileParameter(String path) {
        this.path = path;
    }

    public String getPath(){
        return this.path;
    }

    public URI getURI(){
        return URI.create(getPath());
    }
}
