package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.operators.ModelSource;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.List;

public class JenaModelSource extends ModelSource implements JenaExecutionOperator {

    public JenaModelSource(String inputUrl, String... variableNames) {
        super(inputUrl, variableNames);
    }

    public JenaModelSource(ModelSource that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }
}
