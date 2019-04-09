package org.qcri.rheem.serialize;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.protocol.RheemAutoEnconderFactory;
import org.qcri.rheem.serialize.protocol.RheemDecode;
import org.qcri.rheem.serialize.protocol.RheemEncode;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtobufFactory;
import org.qcri.rheem.serialize.protocol.protobuf.store.ProtoBufStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.RheemStoreWriter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;


public class RheemSerializer<Protocol> {

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
        this.encoder = autoEnconderFactory.buildEncode();
        this.decoder = autoEnconderFactory.buildDecode();
        this.protocolClass = autoEnconderFactory.getProtocolClass();

        this.reader = rheemStoreFactory.buildReader();
        this.writer = rheemStoreFactory.buildWriter();
    }

    public RheemSerialized<Protocol> getSerialized(RheemPlan rheemPlan){
        return this.encoder.enconde(rheemPlan);
    }

    public RheemSerialized<Protocol> getSerialized(RheemIdentifier identifier){
        return this.reader.read(identifier);
    }

    public boolean save(RheemPlan plan){
        RheemSerialized<Protocol> serialized = getSerialized(plan);
        return this.writer.save(serialized);
    }

    public RheemPlan recovery(RheemIdentifier identifier){
        RheemSerialized<Protocol> serialized = this.getSerialized(identifier);
        return this.decoder.decode(serialized);
    }


    public static void main(String... args) throws IOException, URISyntaxException {
        //RheemPlan plan = org.qcri.rheem.serialize.tmp.Main.createRheemPlan("file:///lalal", new ArrayList<>());

        RheemSerializer serializer = new RheemSerializer();

        //serializer.save(plan);

        RheemPlan plan = serializer.recovery(new RheemIdentifier("4b2d11a0-a3c4-4c2c-8879-fc64b4166e64"));



    }



}
