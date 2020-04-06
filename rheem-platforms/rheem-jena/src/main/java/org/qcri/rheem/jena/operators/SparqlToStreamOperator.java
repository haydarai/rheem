package org.qcri.rheem.jena.operators;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.json.JSONObject;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.core.optimizer.OptimizationContext;
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
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
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

        Model model = RDFDataMgr.loadModel(input.getModelUrl()) ;

        Tuple3<String, String, String> variables = input.getTriple();
        Triple triple = new Triple(
                Var.alloc(variables.getField0()),
                Var.alloc(variables.getField1()),
                Var.alloc(variables.getField2())
        );
        BasicPattern bp = new BasicPattern();
        bp.add(triple);
        Op op = new OpBGP(bp);

        QueryIterator queryIterator = Algebra.exec(op, model.getGraph());
        List<String> resultVars = variables.asList();
        ResultSet resultSet = new ResultSetStream(resultVars, model, queryIterator);

        List<QuerySolution> result = ResultSetFormatter.toList(resultSet);

        queryIterator.close();

        Stream<Record> resultSetStream = result.stream().map(qs -> {
            final int recordWidth = resultVars.size();
            Object[] values = new Object[recordWidth];
            for (int i = 0; i < recordWidth; i++) {
                values[i] = qs.get(resultVars.get(i));
            }
            return new Record(values);
        });
        output.accept(resultSetStream);

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
}
