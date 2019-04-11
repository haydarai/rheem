package org.qcri.rheem.serialize;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.protocol.RheemAutoEnconderFactory;
import org.qcri.rheem.serialize.protocol.RheemDecode;
import org.qcri.rheem.serialize.protocol.RheemEncode;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtobufFactory;
import org.qcri.rheem.serialize.protocol.protobuf.store.ProtoBufStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.RheemStoreWriter;


public class RheemSerializer<Protocol> {

    private Configuration configuration;
    private Class<Protocol> protocolClass;
    private RheemEncode<Protocol> encoder;
    private RheemDecode<Protocol> decoder;

    private RheemStoreReader reader;
    private RheemStoreWriter writer;

    public RheemSerializer(){
        //TODO recovery from configuration;
        this(new RheemProtobufFactory(), new ProtoBufStoreFactory());
    }

    public RheemSerializer(RheemAutoEnconderFactory autoEnconderFactory, RheemStoreFactory rheemStoreFactory){
        this.configuration = new Configuration();
        this.encoder = autoEnconderFactory.buildEncode(this.configuration);
        this.decoder = autoEnconderFactory.buildDecode(this.configuration);
        this.protocolClass = autoEnconderFactory.getProtocolClass();

        this.reader = rheemStoreFactory.buildReader(this.configuration);
        this.writer = rheemStoreFactory.buildWriter(this.configuration);
    }

    public RheemSerialized<Protocol> getSerialized(RheemPlan rheemPlan){
        return this.encoder.enconde(rheemPlan);
    }

    public RheemSerialized<Protocol> getSerialized(RheemIdentifier identifier){
        return this.reader.read(identifier);
    }

    public RheemIdentifier save(RheemPlan plan){
        RheemSerialized<Protocol> serialized = getSerialized(plan);
        this.writer.save(serialized);
        return serialized.getId();
    }

    public RheemPlan recovery(RheemIdentifier identifier){
        RheemSerialized<Protocol> serialized = this.getSerialized(identifier);
        return this.decoder.decode(serialized);
    }


}
