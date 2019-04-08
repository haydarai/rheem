package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.TransformationDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Wrapper for {@Link KeySelector}
 */
public class KeySelectorFunction<T, K> implements KeySelector<T, K>, ResultTypeQueryable<K>, Serializable {

    public Function<T, K> impl;

    public Class<K> key;

    public TypeInformation<K> typeInformation;

    public KeySelectorFunction(TransformationDescriptor<T, K> transformationDescriptor) {

        this.impl = transformationDescriptor.getJavaImplementation();
        this.key  = transformationDescriptor.getOutputType().getTypeClass();
        System.out.println("keydasdadddddddd: "+this.key);
        //TODO validate this
        //if(this.key.getTypeParameters().length > 0)

        if(this.key == scala.Tuple4.class ){
            try{
                this.typeInformation = new PojoTypeInfo(scala.Tuple4.class, Arrays.asList(
                        new PojoField(scala.Tuple4.class.getField("_1"),  TypeInformation.of(Long.class)),
                        new PojoField(scala.Tuple4.class.getField("_2"),  TypeInformation.of(Long.class)),
                        new PojoField(scala.Tuple4.class.getField("_3"),  TypeInformation.of(Integer.class)),
                        new PojoField(scala.Tuple4.class.getField("_4"),  TypeInformation.of(Integer.class))
                        // new PojoField(Tuple2.class.getField("field1"),  TypeInformation.of(Tuple2.class.getField("field1").getType() ))
                ));
            } catch (NoSuchFieldException e) {
                this.typeInformation = null;
            }
        }else{
            this.typeInformation = TypeInformation.of(this.key);
        }

        //}else {
        //    this.typeInformation = TypeInformation.of(this.key);
        //}
        //this.typeInformation = TypeInformation.of(this.key);
    }

    public K getKey(T object){
        return this.impl.apply(object);
    }


    @Override
    public TypeInformation<K> getProducedType(){
        return this.typeInformation;
    }
}
