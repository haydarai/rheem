package org.qcri.rheem.experiment.enviroment;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.qcri.rheem.experiment.ExperimentException;

public class JavaEnviroment extends EnviromentExecution<Stream> {
    @Override
    public Stream getEnviroment() {
        return null;
    }



}
