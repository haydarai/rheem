package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.qcri.rheem.spark.operators.SparkTextFileSource;

import java.io.*;
import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
 */
public class SparkTextFileSourceProfiler extends SparkSourceProfiler {

    private final String fileUrl;

    public SparkTextFileSourceProfiler(Configuration configuration,
                                       Supplier<?> dataQuantumGenerator) {
        //this(configuration.getStringProperty("rheem.profiler.datagen.url"), configuration, dataQuantumGenerator);
        this(configuration.getStringProperty("rheem.core.log.syntheticData"),configuration,dataQuantumGenerator);
    }

    private SparkTextFileSourceProfiler(String fileUrl,
                                        Configuration configuration,
                                        Supplier<?> dataQuantumGenerator) {
        super((Supplier<SparkExecutionOperator> & Serializable) () -> {SparkTextFileSource op= new SparkTextFileSource("file:/"+fileUrl); op.setName("SparkTextFileSource"); return op;},
                configuration, dataQuantumGenerator);
        this.fileUrl = fileUrl;
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;

        /*
        // Obtain access to the file system.
        final FileSystem fileSystem = FileSystems.getFileSystem(this.fileUrl).orElseThrow(
                () -> new RheemException(String.format("File system of %s not supported.", this.fileUrl))
        );

        // Try to delete any existing file.
        try {
            if (!fileSystem.delete(this.fileUrl, true)) {
                this.logger.warn("Could not delete {}.", this.fileUrl);
            }
        } catch (IOException e) {
            this.logger.error(String.format("Deleting %s failed.", this.fileUrl), e);
        }


        // Generate and write the test data.
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(this.fileUrl),
                        "UTF-8"
                )
        )) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (long i = 0; i < inputCardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        } catch (Exception e) {
            throw new RheemException(String.format("Could not write test data to %s.", this.fileUrl), e);
        }
        */
        try {
            File file = new File(this.fileUrl);
            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new RheemException("Could not initialize log repository.");
            }
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));

            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);

            for (long i = 0; i < inputCardinality; i++) {
                if (i%(inputCardinality/2)==0)

                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        } catch (RheemException e) {
            throw e;
        } catch (Exception e) {
            throw new RheemException(String.format("Cannot write to %s.", this.fileUrl), e);
        }
    }

    @Override
    public void cleanUp() {
        super.cleanUp();

        FileSystems.getFileSystem(this.fileUrl).ifPresent(fs -> {
                    try {
                        fs.delete(this.fileUrl, true);
                    } catch (IOException e) {
                        this.logger.error(String.format("Could not delete profiling file %s.", this.fileUrl), e);
                    }
                }
        );
    }
}
