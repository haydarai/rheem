package org.qcri.rheem.jena.operators;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Collections;
import java.util.List;

public interface JenaExecutionOperator extends ExecutionOperator {

    @Override
    default JenaPlatform getPlatform() {
        return JenaPlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSparqlQueryChannelDescriptor());
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSparqlQueryChannelDescriptor());
    }
}
