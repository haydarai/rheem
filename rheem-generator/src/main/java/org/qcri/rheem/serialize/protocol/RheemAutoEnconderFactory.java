package org.qcri.rheem.serialize.protocol;

public interface RheemAutoEnconderFactory<Protocol> {


    RheemEncode<Protocol> buildEncode();

    RheemDecode<Protocol> buildDecode();

    Class<Protocol> getProtocolClass();


}
