package org.qcri.rheem.profiler.data;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class that generates UDFs
 */
public class UdfGenerators <Input, Output> {

    protected final BasicDataUnitType<Input> inputType;

    protected final BasicDataUnitType<Output> outputType;

    public UdfGenerators(BasicDataUnitType<Input> inputType, BasicDataUnitType<Output> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public static FunctionDescriptor.SerializableFunction<Integer, Integer> mapIntUDF( int complexity,  int dataQuataSize, long... complexityCoef) {
            switch (complexity) {
                case 1:
                    // O(n) UDF complexity with linear cost
                    return i -> i * 5;
                case 12:
                    // O(n) UDF complexity with log cost
                    return i -> i * i + (int) Math.log(i);
                case 13:
                    // O(n) UDF complexity with exp cost
                    return i -> (int) Math.exp(i);
                //case 4:
                //    return i -> (int) Math.cos(i);
                case 2:
                    //if (complexityCoef.length==1)
                        // O(log(n)) UDF complexity
                    return i -> { int iterations = 0; for (int count = 1; count<= dataQuataSize; count=count*2) {iterations = iterations + i * 5;} return iterations;};

                //else
                        //return i -> i * 5;
                case 3:
                    return i -> { int iterations = 0; for (int count = 1; count<= dataQuataSize; count++) {iterations = iterations + i * 5;} return iterations;};
                    //if (complexityCoef.length==1)
                        // O(n2) UDF complexity

                    //else
                    //    return i -> i * 5;
                        //new RheemException("Missing UDF complexity coefficient!");
                default:
                    return null;
            }
    }

    public static FunctionDescriptor.SerializableFunction<List,List> mapIntListUDF(int complexity,  int dataQuataSize, long... coef){
        return  i -> (List<Integer>)i.stream()
                .map(el -> (Integer) UdfGenerators.mapIntUDF(complexity,dataQuataSize).apply((Integer) el))
                .collect(Collectors.toList());
    }



    public static FunctionDescriptor.SerializableFunction<String, String> mapStringUDF( int complexity,  int dataQuataSize, long... coef) {
        switch (complexity) {
            case 1:
                return String::new;
            case 2:
                return i -> {String tempString = "**";for (int count = 1; count<= dataQuataSize; count=count*2) {
                    if (i!= null)tempString= tempString+i;} return tempString;};
                //tmpInt=tmpInt+5*(i+ tempString).length();
            //tempString= tempString +i;
            case 3:
                return i -> {String tempString = "**"; for (int count = 1; count<= dataQuataSize; count++) {if (i!= null)tempString= tempString+i;} return tempString;};
            default:
                return null;
        }
    }


    public static PredicateDescriptor.SerializablePredicate <Integer> filterIntUDF(int complexity,  int dataQuataSize, long... coef) {
        switch (complexity) {
            case 1:
                return i ->  {
                    //mapIntUDF(complexity).apply(i);
                    //return (i & 1)==0;
                    mapIntUDF(complexity,dataQuataSize).apply(i);
                    return true;
                };
            case 2:
                return i -> {
                    mapIntUDF(complexity,dataQuataSize).apply(i);
                    return (i > 0);
                };
            case 3:
                return i -> {
                    mapIntUDF(complexity,dataQuataSize).apply(i);
                    return (i > 0 & ((i & 1) == 0));
                };
            default:
                return null;
        }
    }

    public static PredicateDescriptor.SerializablePredicate <List> filterIntListUDF(int complexity,  int dataQuataSize, long... coef) {
        switch (complexity) {
            case 1:
                //i ->  ((Integer)i.get(0) & 1)==0;
                return i -> {
                    //i.stream().map(el -> mapIntUDF(complexity).apply((Integer)el));
                    mapIntListUDF(complexity,dataQuataSize).apply(i);
                    //return true;
                    return (i.stream().allMatch(x -> ((int)x & 1)==0));
                };
            case 2:
                return i -> {
                    //i.stream().map(el -> (int) Math.exp((int)el));
                    mapIntListUDF(complexity,dataQuataSize).apply(i);
                    return (i.stream().allMatch(x -> (((int)x & 1)==0) & ((int)x > 0)));
                };
            case 3:
                return i -> {
                    //i.stream().map(el -> mapIntUDF(complexity).apply((Integer)el));
                    mapIntListUDF(complexity,dataQuataSize).apply(i);
                    //return true;
                    return (i.stream().allMatch(x -> ((((int)x & 1)==0) & ((int)x > 0)) | (((int)x > 1000))));
                };
            default:
                return null;
        }
    }

    private static Integer highCompexity(int dataQuataSize, int input){
        int iterations=0;
        for (int count = 1; count<= dataQuataSize; count++) {
            //iterations = iterations + (int) Math.log(input);
            iterations = iterations + (int) Math.log(input);
        }
        return iterations;
    }


    interface TwoIntArgInterface<Input, Output> {

        public Output operation(Input a);
    }

}

/*
public class UdfGenerators <Input, Output> {

    protected final BasicDataUnitType<Input> inputType;

    protected final BasicDataUnitType<Output> outputType;

    public UdfGenerators(BasicDataUnitType<Input> inputType, BasicDataUnitType<Output> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public FunctionDescriptor.SerializableFunction<Input, Output> mapUDF(Class<Input> inClass, int complexity, long... coef){
        if (inClass == )
        switch (inClass.getComponentType()){
            case (Class<?>) Integer.class.getComponentType():
                switch (complexity){
                    case 1:
                        return i -> 5 + i.value;
                    case 2:
                        return (Object i) -> {
                            return (i) ->  5 * i;
                        };
                }
        }

        String a;
        Pattern p;

        p.

            return i -> i;
    }

    /} */


/*
SECOND VERSION

public class UdfGenerators <Input, Output> {

    protected final BasicDataUnitType<Input> inputType;

    protected final BasicDataUnitType<Output> outputType;

    public UdfGenerators(BasicDataUnitType<Input> inputType, BasicDataUnitType<Output> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public FunctionDescriptor.SerializableFunction<Input, Output> mapUDF(BasicDataUnitType<Input> inputType, int complexity, long... coef){
        if ((inputType.getTypeClass() == Integer.class)&&(outputType.getTypeClass() == Integer.class)){
            switch (complexity){
                case 1:
                    return i -> 5 + i;
                case 2:
                    return (Object i) -> {
                        return (i) ->  5 * i;
                    };
            }
        }
        Class<Input> in = inputType.getTypeClass();

    }


}

 */