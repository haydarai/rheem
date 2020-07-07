package org.qcri.rheem.java.operators.graph;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.SingleSourceShortestPathOperator;
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

public class JavaSingleSourceShortestPathOperator extends SingleSourceShortestPathOperator implements
        JavaExecutionOperator {

    private final long sourceId;
    private final Set<Long> nodes;
    private List<Tuple2<Long, Long>> edges;
    private final Set<Long> settledNodes;
    private final Set<Long> unSettledNodes;
    private final Map<Long, Long> predecessors;
    private final TLongFloatMap distances;

    public JavaSingleSourceShortestPathOperator(SingleSourceShortestPathOperator that) {
        super(that);
        this.sourceId = that.getSourceId();
        this.nodes = new HashSet<>();
        this.edges = new ArrayList<>();
        this.settledNodes = new HashSet<>();
        this.unSettledNodes = new HashSet<>();
        this.predecessors = new HashMap<>();
        this.distances = new TLongFloatHashMap();
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        this.edges = new ArrayList<>(input.provideCollection());
        for (Tuple2<Long, Long> edge : this.edges) {
            nodes.add(edge.field0);
            nodes.add(edge.field1);
            distances.put(edge.field0, Float.POSITIVE_INFINITY);
            distances.put(edge.field1, Float.POSITIVE_INFINITY);
        }
        distances.put(sourceId, 0);
        unSettledNodes.add(sourceId);

        while (!unSettledNodes.isEmpty()) {
            Long node = getMinimum(unSettledNodes);
            settledNodes.add(node);
            unSettledNodes.remove(node);
            findMinimalDistances(node);
        }

        final Stream<Tuple2<Long, Float>> ssspStream = this.stream(distances);

        output.accept(ssspStream);

        return ExecutionOperator.modelQuasiEagerExecution(inputs, outputs, operatorContext);
    }

    private Stream<Tuple2<Long, Float>> stream(TLongFloatMap map) {
        final TLongFloatIterator tLongFloatIterator = map.iterator();
        Iterator<Tuple2<Long, Float>> iterator = new Iterator<Tuple2<Long, Float>>() {
            @Override
            public boolean hasNext() {
                return tLongFloatIterator.hasNext();
            }

            @Override
            public Tuple2<Long, Float> next() {
                tLongFloatIterator.advance();
                return new Tuple2<>(tLongFloatIterator.key(), tLongFloatIterator.value());
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    private void findMinimalDistances(Long node) {
        List<Long> adjacentNodes = getNeighbors(node);
        for (Long target : adjacentNodes) {
            if (getShortestDistance(target) > getShortestDistance(node)
                    + getDistance(node, target)) {
                distances.put(target, getShortestDistance(node)
                        + getDistance(node, target));
                predecessors.put(target, node);
                unSettledNodes.add(target);
            }
        }
    }

    private int getDistance(Long node, Long target) {
        for (Tuple2<Long, Long> edge : edges) {
            if (edge.getField0().equals(node)
                    && edge.getField1().equals(target)) {
                return 1;
            }
        }
        throw new RuntimeException("Should not happen");
    }

    private List<Long> getNeighbors(Long node) {
        List<Long> neighbors = new ArrayList<>();
        for (Tuple2<Long, Long> edge : edges) {
            if (edge.getField0().equals(node)
                    && !isSettled(edge.getField1())) {
                neighbors.add(edge.getField1());
            }
        }
        return neighbors;
    }

    private boolean isSettled(Long vertex) {
        return settledNodes.contains(vertex);
    }

    private Long getMinimum(Set<Long> vertexes) {
        Long minimum = null;
        for (Long vertex : vertexes) {
            if (minimum == null) {
                minimum = vertex;
            } else {
                if (getShortestDistance(vertex) < getShortestDistance(minimum)) {
                    minimum = vertex;
                }
            }
        }
        return minimum;
    }

    private float getShortestDistance(Long destination) {
        Float d = distances.get(destination);
        if (d == null) {
            return Float.POSITIVE_INFINITY;
        } else {
            return d;
        }
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.sssp.load";
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
