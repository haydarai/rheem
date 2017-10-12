package org.qcri.rheem.profiler.java;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.java.operators.JavaTextFileSource;
import org.qcri.rheem.profiler.core.api.OperatorProfiler;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for sources.
 */
public class JavaTextFileSourceProfiler extends JavaSourceProfiler {

    private File tempFile;

    private String fileUrl;

    public JavaTextFileSourceProfiler(Supplier<String> dataQuantumGenerator, String fileUrl) {
        //this.setFileUrl(fileUrl);
        super(() -> new JavaTextFileSource("file:/"+fileUrl), dataQuantumGenerator);
        //this.fileUrl = fileUrl;
        this.setFileUrl(fileUrl);
    }

    @Override
    public void setUpSourceData(long cardinality) throws Exception {
/*
        if (this.tempFile != null) {
            if (!this.tempFile.delete()) {
                this.logger.warn("Could not delete {}.", this.tempFile);
            }
        }
        this.tempFile = File.createTempFile("rheem-java", "txt");
        URI uri = URI.create(this.fileUrl);
        File inputFile = new File(uri.getRawPath());


        // Create a file if does not exist
        if (!inputFile.exists()) {
            inputFile.createNewFile();
        }
        FileOutputStream fooStream = new FileOutputStream(this.tempFile, false);

        // Create input data.
        BufferedWriter writer1 = new BufferedWriter(new FileWriter(this.tempFile));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile))) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (int i = 0; i < cardinality; i++) {
                writer1.write(supplier.get().toString());
                writer1.write('\n');
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        }
        */

        // write
        try {
            File file = new File(this.fileUrl);

            // Try to delete existing file
            try{
                file.delete();
            } catch (Exception e){
                this.logger.error(String.format("Deleting %s failed.", this.fileUrl), e);
            }

            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new RheemException("Could not initialize cardinality repository.");
            }
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));

            Supplier<?> supplier = this.dataQuantumGenerators.get(0);

            for (long i = 0; i < cardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
            writer.close();
            //supplier=null;
            //writer = null;
        } catch (RheemException e) {
            throw e;
        } catch (Exception e) {
            throw new RheemException(String.format("Cannot write to %s.", this.fileUrl), e);
        }
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }

    @Override
    protected long provideDiskBytes() {
        return this.tempFile.length();
    }


}
