package org.qcri.rheem.experiment.utils.udf;

import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.utils.udf.type.UDF;

import java.io.Serializable;
import java.util.HashMap;

public class UDFs implements Serializable {

    private HashMap<String, UDF> map_UDF;

    public UDFs() {
        this.map_UDF = new HashMap<>();
    }

    public void addUDF(String name, UDF udf){
        this.map_UDF.put(name, udf);
    }

    public UDF getUDF(String name){
        if( ! this.map_UDF.containsKey(name) ){
            throw new ExperimentException(
                String.format(
                    "the %s not exist in the udf definitions %s",
                    name,
                    this.map_UDF.keySet()
                )
            );
        }
        return this.map_UDF.get(name);
    }
}
