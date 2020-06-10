package org.qcri.rheem.java.operators.graph;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.DegreeCentralityOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaDegreeCentralityOperator extends DegreeCentralityOperator implements JavaExecutionOperator {

    public JavaDegreeCentralityOperator(DegreeCentralityOperator that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        final Collection<Tuple2<Long, Long>> edges = input.provideCollection();
        final TLongIntMap degreeCentrality =this.degreeCentrality(edges);
        final Stream<Tuple2<Long, Integer>> degreeCentralityStream = this.stream(degreeCentrality);

        output.accept(degreeCentralityStream);

        return ExecutionOperator.modelQuasiEagerExecution(inputs, outputs, operatorContext);
    }

    private TLongIntMap degreeCentrality(Collection<Tuple2<Long, Long>> edgeDataSet) {
        TLongIntMap degrees = new TLongIntHashMap();
        for (Tuple2<Long, Long> edge : edgeDataSet) {
            degrees.adjustOrPutValue(edge.field0, 1, 1);
            degrees.adjustOrPutValue(edge.field1, 1, 1);
        }

        return degrees;
    }

    private Stream<Tuple2<Long, Integer>> stream(TLongIntMap map) {
        final TLongIntIterator tLongIntIterator = map.iterator();
        Iterator<Tuple2<Long, Integer>> iterator = new Iterator<Tuple2<Long, Integer>>() {
            @Override
            public boolean hasNext() {
                return tLongIntIterator.hasNext();
            }

            @Override
            public Tuple2<Long, Integer> next() {
                tLongIntIterator.advance();
                return new Tuple2<>(tLongIntIterator.key(), tLongIntIterator.value());
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.pagerank.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
