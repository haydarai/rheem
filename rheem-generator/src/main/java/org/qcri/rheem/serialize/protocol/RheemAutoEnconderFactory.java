package org.qcri.rheem.serialize.protocol;

import org.qcri.rheem.core.api.Configuration;

public interface RheemAutoEnconderFactory<Protocol> {


    RheemEncode<Protocol> buildEncode(Configuration conf);

    RheemDecode<Protocol> buildDecode(Configuration conf);

    Class<Protocol> getProtocolClass();


}
