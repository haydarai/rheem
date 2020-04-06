package org.qcri.rheem.jena.channels;

import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.jena.operators.SparqlToStreamOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Arrays;
import java.util.Collection;

public class ChannelConversions {

    public static final ChannelConversion SPARQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            JenaPlatform.getInstance().getSparqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SparqlToStreamOperator(JenaPlatform.getInstance())
    );

    public static Collection<ChannelConversion> ALL = Arrays.asList(
            SPARQL_TO_STREAM_CONVERSION
    );
}
