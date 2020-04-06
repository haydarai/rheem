package org.qcri.rheem.jena.channels;

import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Objects;

public class SparqlQueryChannel extends Channel {

    public SparqlQueryChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
    }

    public SparqlQueryChannel(SparqlQueryChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new SparqlQueryChannel(this);
    }

    @Override
    public SparqlQueryChannel.Instance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }

    public class Instance extends AbstractChannelInstance {

        private String modelUrl = null;

        private Tuple3<String, String, String> triple = null;

        /**
         * Creates a new instance and registers it with its {@link Executor}.
         *
         * @param executor                that maintains this instance
         * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
         *                                {@link ExecutionOperator}
         * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
         */
        protected Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        @Override
        public Channel getChannel() {
            return SparqlQueryChannel.this;
        }

        @Override
        protected void doDispose() throws Throwable {

        }

        public String getModelUrl() {
            return modelUrl;
        }

        public void setModelUrl(String modelUrl) {
            this.modelUrl = modelUrl;
        }

        public Tuple3<String, String, String> getTriple() {
            return triple;
        }

        public void setTriple(Tuple3<String, String, String> triple) {
            this.triple = triple;
        }

        public void setTriple(String... variables) {
            this.triple = new Tuple3<>(variables[0], variables[1], variables[2]);
        }
    }

    public static class Descriptor extends ChannelDescriptor {

        private final JenaPlatform platform;

        public Descriptor(JenaPlatform platform) {
            super(SparqlQueryChannel.class, false, false);
            this.platform = platform;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(platform, that.platform);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), platform);
        }
    }
}
