package org.qcri.rheem.core.store;

import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.util.MultiMap;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

/**
 * Created by bertty on 16-11-17.
 */
public abstract class OperatorStore<Type, TypeConexion> {

    protected String name_job;

    protected Collection<Type> rheem_operators;

    protected Collection<Type> execution_operators;

    protected MultiMap<Type, TypeConexion> large_conexion;

    protected MultiMap<TypeConexion, Type> inverse_large_conexion;


    public OperatorStore(){
        this.name_job = "job_"+ generateName();
    }

    public OperatorStore(String name_job){
        this.name_job = name_job;
    }


    private String generateName() {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] array = md.digest( generateBytes() );
            return Base64.getEncoder().encodeToString(array);
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }

    private byte[] generateBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong((new Random()).nextLong());
        return buffer.array();
    }

}
