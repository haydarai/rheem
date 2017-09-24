package org.qcri.rheem.profiler.java;

import org.qcri.rheem.java.operators.JavaTextFileSource;
import org.qcri.rheem.profiler.core.api.OperatorProfiler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.net.URI;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for sources.
 */
public class JavaTextFileSourceProfiler extends JavaSourceProfiler {

    private File tempFile;

    private String fileUrl;

    public JavaTextFileSourceProfiler(Supplier<String> dataQuantumGenerator, String fileUrl) {
        //this.setFileUrl(fileUrl);
        super(() -> new JavaTextFileSource(fileUrl), dataQuantumGenerator);
        this.setFileUrl(fileUrl);
    }

    @Override
    public void setUpSourceData(long cardinality) throws Exception {
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
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }

    @Override
    protected long provideDiskBytes() {
        return this.tempFile.length();
    }


}
