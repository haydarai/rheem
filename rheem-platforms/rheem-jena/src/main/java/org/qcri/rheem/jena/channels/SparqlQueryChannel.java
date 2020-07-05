package org.qcri.rheem.jena.channels;

import org.apache.jena.sparql.algebra.Op;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.ArrayList;
import java.util.List;
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

        private Op op = null;

        private List<String> projectedFields = new ArrayList<>();

        private List<List<String>> joinOrders = new ArrayList<>();

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

        public Op getOp() {
            return op;
        }

        public void setOp(Op op) {
            this.op = op;
        }

        public List<String> getProjectedFields() {
            return projectedFields;
        }

        public void setProjectedFields(List<String> projectedFields) {
            this.projectedFields = projectedFields;
        }

        public List<List<String>> getJoinOrders() {
            return joinOrders;
        }

        public void setJoinOrders(List<List<String>> joinOrders) {
            this.joinOrders = joinOrders;
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
