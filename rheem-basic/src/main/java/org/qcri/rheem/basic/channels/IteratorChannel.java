package org.qcri.rheem.basic.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.util.Actions;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

/**
 * Represents a {@link Channel} that is realized via a Iterator that permit build differet struct
 */
public class IteratorChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            IteratorChannel.class, false, true
    );

    public IteratorChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private IteratorChannel(IteratorChannel parent) {
        super(parent);
    }


    @Override
    public IteratorChannel copy() {
        return new IteratorChannel(this);
    }

    @Override
    public String toString() {
        return String.format("%s[%s->%s]",
                this.getClass().getSimpleName(),
                this.getProducer() == null ? this.getProducerSlot() : this.getProducer(),
                this.getConsumers()
        );
    }

    @Override
    public ChannelInstance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        // NB: File channels are not inherent to a certain Platform, therefore are not tied to the executor.
        return new IteratorChannel.Instance(producerOperatorContext, producerOutputIndex);
    }


    /**
     * {@link ChannelInstance} implementation for {@link FileChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Iterator<?> iterator;

        private FunctionDescriptor.SerializableFunction<?, ?> transformation;

        /**
         * Creates a new instance.
         * @param producerOperatorContext
         * @param producerOutputIndex
         */
        protected Instance(OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(null, producerOperatorContext, producerOutputIndex);
            this.iterator = null;
            this.transformation = null;
        }

        @Override
        public IteratorChannel getChannel() {
            return IteratorChannel.this;
        }

        @Override
        public void doDispose() throws RheemException {
            this.iterator = null;
        }

        @SuppressWarnings("unchecked")
        public <Type> Iterator<Type> provideIterator() {
            return (Iterator<Type>) this.iterator;
        }

        public <Type, TypeOutput> FunctionDescriptor.SerializableFunction<Type, TypeOutput> provideTransformation(){
            return (FunctionDescriptor.SerializableFunction<Type, TypeOutput>) this.transformation;
        }

        public <Type, TypeOutput> void accept(Iterator<Type> iterator, Function<Type, TypeOutput> function){
            this.iterator = iterator;
            this.transformation = (FunctionDescriptor.SerializableFunction<?, ?>) function;
        }
    }
}
