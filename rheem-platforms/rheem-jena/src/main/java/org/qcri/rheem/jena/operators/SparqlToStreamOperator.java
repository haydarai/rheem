package org.qcri.rheem.jena.operators;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.tdb2.TDB2Factory;
import org.json.JSONObject;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.jena.channels.SparqlQueryChannel;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SparqlToStreamOperator extends UnaryToUnaryOperator<Record, Record> implements JavaExecutionOperator, JsonSerializable {

    private final JenaPlatform jenaPlatform;

    public SparqlToStreamOperator(JenaPlatform jenaPlatform) {
        this(jenaPlatform, DataSetType.createDefault(Record.class));
    }

    public SparqlToStreamOperator(JenaPlatform jenaPlatform, DataSetType<Record> dataSetType) {
        super(dataSetType, dataSetType, false);
        this.jenaPlatform = jenaPlatform;
    }

    protected SparqlToStreamOperator(SparqlToStreamOperator that) {
        super(that);
        this.jenaPlatform = that.jenaPlatform;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs
        final SparqlQueryChannel.Instance input = (SparqlQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        List<String> resultVars = input.getProjectedFields();

        Dataset ds = TDB2Factory.connectDataset(input.getModelUrl());
        final Stream<QuerySolution>[] resultStream = new Stream[]{Stream.empty()};

        Runnable runnable = () -> {
            ds.begin(ReadWrite.READ);
            try (QueryExecution qe = QueryExecutionFactory.create(OpAsQuery.asQuery(input.getOp()), ds)) {
                ResultSet resultSet = qe.execSelect();

                Stream.Builder<QuerySolution> builder = Stream.builder();
                while (resultSet.hasNext()) {
                    QuerySolution querySolution = resultSet.nextSolution();
                    materialize(querySolution);
                    builder.add(querySolution);
                }

                resultStream[0] = builder.build();
            } finally {
                ds.end();
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<List<String>> joinOrders = input.getJoinOrders();

        if (joinOrders.isEmpty()) {
            Stream<Record> resultSetStream = resultStream[0]
                    .filter(querySolution -> {
                        Iterator<String> varNames = querySolution.varNames();
                        while (varNames.hasNext()) {
                            String varName = varNames.next();
                            if (querySolution.get(varName) == null) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .map(qs -> {
                        final int recordWidth = resultVars.size();
                        Object[] values = new Object[recordWidth];
                        for (int i = 0; i < recordWidth; i++) {
                            values[i] = qs.get(resultVars.get(i)).toString();
                        }
                        return new Record(values);
                    });

            output.accept(resultSetStream);

            ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
            queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "rheem.jena.sparqltostream.load.query", javaExecutor.getConfiguration()
            ));
            queryLineageNode.addPredecessor(input.getLineage());
            ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
            outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "rheem.jena.sparqltostream.load.output", javaExecutor.getConfiguration()
            ));
            output.getLineage().addPredecessor(outputLineageNode);
        } else {
            Stream<Tuple2> finalResult = resultStream[0].map(qs -> {
                int recordWidth = joinOrders.get(0).size();
                Object[] values = new Object[recordWidth];
                for (int i = 0; i < joinOrders.get(0).size(); i++) {
                    String currentVar = joinOrders.get(0).get(i);
                    int projectedField = resultVars.indexOf(currentVar);
                    values[i] = qs.get(resultVars.get(projectedField)).toString();
                }
                Object prevRecord = new Record(values);

                Tuple2 tuple2 = new Tuple2();

                for (int i = 1; i < joinOrders.size(); i++) {
                    int recWidth = joinOrders.get(i).size();
                    Object[] vals = new Object[recWidth];
                    for (int j = 0; j < joinOrders.get(i).size(); j++) {
                        String currentVar = joinOrders.get(i).get(j);
                        int projectedField = resultVars.indexOf(currentVar);
                        vals[j] = qs.get(resultVars.get(projectedField)).toString();
                    }
                    Record newRecord = new Record(vals);

                    tuple2 = new Tuple2(prevRecord, newRecord);
                    prevRecord = tuple2;
                }

                return tuple2;
            });

            output.accept(finalResult);

            ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
            queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "rheem.jena.join.load.query", javaExecutor.getConfiguration()
            ));
            queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "rheem.jena.filter.load.query", javaExecutor.getConfiguration()
            ));
            queryLineageNode.addPredecessor(input.getLineage());
            ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
            outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "rheem.jena.join.load.output", javaExecutor.getConfiguration()
            ));
            output.getLineage().addPredecessor(outputLineageNode);
        }

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jenaPlatform.getSparqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public JSONObject toJson() {
        return new JSONObject().put("platform", this.jenaPlatform.getClass().getCanonicalName());
    }

    private void materialize(QuerySolution qs) {
        for (Iterator<String> iter = qs.varNames(); iter.hasNext(); ) {
            String vn = iter.next();
            RDFNode n = qs.get(vn);
        }
    }
}
