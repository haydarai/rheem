package org.qcri.rheem.flink.operators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.compiler.RheemFileOutputFormat;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * Converts {@link DataSetChannel} into a {@link CollectionChannel}
 */
public class FlinkCollectionSink<Type> extends UnaryToUnaryOperator<Type, Type>
        implements FlinkExecutionOperator  {
    public FlinkCollectionSink(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FlinkExecutor flinkExecutor, OptimizationContext.OperatorContext operatorContext) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();
        final String tempDir = flinkExecutor.getConfiguration().getStringProperty("rheem.basic.tempdir")+"/" + UUID.randomUUID().toString();

        final DataSink<Type> tDataSink = input.<Type>provideDataSet()
                .write(new RheemFileOutputFormat<Type>(tempDir), tempDir, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(flinkExecutor.getNumDefaultPartitions());


        flinkExecutor.fee.execute();

        org.qcri.rheem.core.util.fs.FileSystem fs = FileSystems.getFileSystem(tempDir).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", tempDir))
        );

        Stream<Type> lines;
        if(fs.isDirectory(tempDir)){
            lines = fs.listChildren(tempDir)
                    .stream()
                    .filter( p -> ! fs.isDirectory(p))
                    .filter( p -> ! p.contains("_SUCCESS"))
                    .filter( p -> ! p.endsWith(".crc"))
                    .flatMap(p -> {
                        final String actualInputPath = FileSystems.findActualSingleInputPath(p);
                        try {
                            FlinkCollectionSink.SequenceFileIterator<Type> sequenceFileIterator = new FlinkCollectionSink.SequenceFileIterator<>(actualInputPath);
                            return StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(
                                            sequenceFileIterator,
                                            0
                                    ), false
                            );
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return Stream.empty();
                    });
        }else {
            final String actualInputPath = FileSystems.findActualSingleInputPath(tempDir);
            FlinkCollectionSink.SequenceFileIterator<Type> sequenceFileIterator = new FlinkCollectionSink.SequenceFileIterator<>(actualInputPath);
            lines = StreamSupport.stream(Spliterators.spliteratorUnknownSize(sequenceFileIterator, 0), false);
        }


        output.accept(lines.collect(Collectors.toList()));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);

    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.collect.load";
    }

    private static class SequenceFileIterator<T> implements Iterator<T>, AutoCloseable, Closeable {

        private SequenceFile.Reader sequenceFileReader;

        private final NullWritable nullWritable = NullWritable.get();

        private final BytesWritable bytesWritable = new BytesWritable();

        private Object[] nextElements;

        private ArrayList nextElements_cole;

        private int nextIndex;

        SequenceFileIterator(String path) throws IOException {
            final SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path(path));
            this.sequenceFileReader = new SequenceFile.Reader(new org.apache.hadoop.conf.Configuration(true), fileOption);
            Validate.isTrue(this.sequenceFileReader.getKeyClass().equals(NullWritable.class));
            Validate.isTrue(this.sequenceFileReader.getValueClass().equals(BytesWritable.class));
            this.tryAdvance();
        }

        private void tryAdvance() {
            if (this.nextElements != null && ++this.nextIndex < this.nextElements.length) return;
            if (this.nextElements_cole != null && ++this.nextIndex < this.nextElements_cole.size()) return;
            try {
                if (!this.sequenceFileReader.next(this.nullWritable, this.bytesWritable)) {
                    this.nextElements = null;
                    return;
                }
                Object tmp = new ObjectInputStream(new ByteArrayInputStream(this.bytesWritable.getBytes())).readObject();
                if(tmp instanceof Collection) {
                    this.nextElements = null;
                    this.nextElements_cole = (ArrayList) tmp;
                }else if(tmp instanceof Object[]){
                    this.nextElements = (Object[]) tmp;
                    this.nextElements_cole = null;
                }else {
                    this.nextElements = new Object[1];
                    this.nextElements[0] = tmp;

                }
                this.nextIndex = 0;
            } catch (IOException | ClassNotFoundException e) {
                this.nextElements = null;
                IOUtils.closeQuietly(this);
                throw new RheemException("Reading failed.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextElements != null || this.nextElements_cole != null;
        }

        @Override
        public T next() {
            Validate.isTrue(this.hasNext());
            @SuppressWarnings("unchecked")
            final T result;
            if(this.nextElements_cole != null){
                result = (T) this.nextElements_cole.get(this.nextIndex);
            }else if (this.nextElements != null) {
                result = (T) this.nextElements[this.nextIndex];
            }else{
                result = null;
            }

            this.tryAdvance();
            return result;
        }

        @Override
        public void close() {
            if (this.sequenceFileReader != null) {
                try {
                    this.sequenceFileReader.close();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).error("Closing failed.", t);
                }
                this.sequenceFileReader = null;
            }
        }
    }
}
