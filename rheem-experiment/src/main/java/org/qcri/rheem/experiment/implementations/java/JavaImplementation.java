package org.qcri.rheem.experiment.implementations.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.stream.Stream;

public abstract class JavaImplementation extends Implementation {
    public JavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void preExecutePlan() {
        System.out.println("Implementation java");
    }

    @Override
    protected RheemResults postExecutePlan() {
        return this.results;
    }

    protected Stream<String> getStreamOfFile(URI uri){
        try {
            FileSystem fs = getFileSyste(uri);
            Path file = new Path(uri);
            if(fs.isDirectory(file)){
                return Arrays.stream( fs.listStatus(file) )
                    .filter( status -> status.isFile() )
                    .flatMap( status -> {
                        try {
                            InputStream in = fs.open(status.getPath());
                            return new BufferedReader(new InputStreamReader(in)).lines();
                        } catch (IOException e) {
                            throw new ExperimentException(e);
                        }
                    });
            }else {
                InputStream in = fs.open(file);
                return new BufferedReader(new InputStreamReader(in)).lines();
            }
        } catch (IOException e) {
            throw new ExperimentException(e);
        }
    }

    protected FileSystem getFileSyste(URI uri){
        try {
            Configuration conf = new Configuration();
            return FileSystem.get(uri, conf);
        } catch (IOException e) {
            throw new ExperimentException(e);
        }
    }

    protected void writeFile(URI uri, Stream<String> stream){
        try {
            FileSystem fs = getFileSyste(uri);
            Path file = new Path(uri);
            FSDataOutputStream outputStream = fs.create(file);
            stream.forEach(
                line -> {
                    try {
                        outputStream.writeChars(line);
                    } catch (IOException e) {
                        throw new ExperimentException(e);
                    }
                }
            );
        } catch (IOException e) {
            throw new ExperimentException(e);
        }


    }
}
