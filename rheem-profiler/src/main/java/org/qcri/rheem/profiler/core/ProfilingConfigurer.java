package org.qcri.rheem.profiler.core;

import org.apache.pig.builtin.SIN;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.profiler.core.api.ProfilingConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 * Generates profiling configuration for a profiling plan
 */
public class ProfilingConfigurer {

    private static List<String> ALL_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("textsource", "collectionsource", "map", "filter", "flatmap", "reduce", "globalreduce", "distinct", "distinct-string",
            "distinct-integer", "sort", "sort-string", "sort-integer", "count", "groupby", "join", "union", "cartesian", "callbacksink", "collect",
            "word-count-split", "word-count-canonicalize", "word-count-count"));

    private static List<String> SOURCE_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("textsource", "collectionsource"));

    private static List<String> Test_UNARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("map", "filter", "flatmap", "reduce", "globalreduce", "distinct",
            "groupby","sort"));

    private static List<String> UNARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("map" ));

    private static List<String> BINARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList( "union"));

    private static List<String> LOOP_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList( "repeat"));

    private static List<String> TEST_LOOP_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList( "doWhile","loop","repeat"));


    private static List<String> Test_BINARY_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList("join", "union", "cartesian"));


    private static List<String> SINK_EXECUTION_OPLERATORS = new ArrayList<String>(Arrays.asList( "callbacksink", "collect"));

    //private static final String DEFAULT_INPUT_CARDINALITIES = "1,100,1000,10000,100000,1000000,10000000,20000000";
    private static final String DEFAULT_INPUT_CARDINALITIES = "10000";

    //private static final String DEFAULT_DATA_QUATA_SIZES = "1,10,100,1000,5000,10000";

    private static final String DEFAULT_DATA_QUATA_SIZES = "100";

    // TODO: replace with actual read functions from a user input file
    private static final String DEFAULT_UDF_COMPLEXITIES = "1";

    private static final String DEFAULT_SELECTIVITY_COMPLEXITIES = "1,2,3";

    // The below can be applied only for unary operator profiling
    private static final String DEFAULT_BINARY_INPUT_RATIOS = "1,10,100";

    //
    private static final String DEFAULT_LOOP_ITERATION_NUMBERS = "10,100";

    private static final List<String> DEFAULT_PLATEFORM = Arrays.asList("java");

    private static final List<DataSetType> DEFAULT_DATATYPE = Arrays.asList(DataSetType.createDefault(String.class));
//DataSetType.createDefault(String.class),DataSetType.createDefault(List.class)

    public static final boolean DEFAULT_BUSHY_GENERATION = true;


    public static String getPlateform() {
        return plateform;
    }

    private static String plateform;


    public static ProfilingConfig exhaustiveProfilingConfig(){

        ProfilingConfig pc = new ProfilingConfig();

        // Initiate configuration
        // TODO: the below should be read from a configuration profiling file
        List<Long> inputCardinality = Arrays.stream(DEFAULT_INPUT_CARDINALITIES.split(",")).map(Long::valueOf).collect(Collectors.toList());
        List<Integer> dataQuantas = Arrays.stream(DEFAULT_DATA_QUATA_SIZES.split(",")).map(Integer::valueOf).collect(Collectors.toList());
        List<Integer> UdfsComplexity = Arrays.stream(DEFAULT_UDF_COMPLEXITIES.split(",")).map(Integer::valueOf).collect(Collectors.toList());
        List<Integer> inputRatio = Arrays.stream(DEFAULT_BINARY_INPUT_RATIOS.split(",")).map(Integer::valueOf).collect(Collectors.toList());

        // Set profiling configuration
        pc.setProfilingPlateform(DEFAULT_PLATEFORM);
        pc.setBushyGeneration(DEFAULT_BUSHY_GENERATION);
        pc.setDataType(DEFAULT_DATATYPE);
        pc.setProfilingPlanGenerationEnumeration("exhaustive");
        pc.setProfilingConfigurationEnumeration("exhaustive");
        pc.setInputCardinality(inputCardinality);
        pc.setDataQuantaSize(dataQuantas);
        pc.setUdfsComplexity(UdfsComplexity);
        pc.setInputRatio(inputRatio);

        // Set execution operators
        pc.setUnaryExecutionOperators(UNARY_EXECUTION_OPLERATORS);
        pc.setBinaryExecutionOperators(BINARY_EXECUTION_OPLERATORS);
        pc.setLoopExecutionOperators(LOOP_EXECUTION_OPLERATORS);
        pc.setSourceExecutionOperators(SOURCE_EXECUTION_OPLERATORS);
        pc.setSinkExecutionOperators(SINK_EXECUTION_OPLERATORS);

        return pc;
    }

    public static LinkedHashMap<Integer,ProfilingConfig> pipelineProfilingConfig(int nodesNumber){
        LinkedHashMap<Integer,ProfilingConfig> pcHashList = new LinkedHashMap();

        ProfilingConfig pc = new ProfilingConfig();

        // Initiate configuration
        // TODO: the below should be read from a configuration profiling file
        pc.setInputCardinality(Arrays.stream(DEFAULT_INPUT_CARDINALITIES.split(",")).map(Long::valueOf).collect(Collectors.toList()));
        pc.setDataQuantaSize(Arrays.stream(DEFAULT_DATA_QUATA_SIZES.split(",")).map(Integer::valueOf).collect(Collectors.toList()));

        pcHashList.put(1,pc);

        // Case of 2 nodes the configuration will apply only on input node
        if (nodesNumber==2)
            return pcHashList;
        for (int i=2;i<nodesNumber;i++){
            pc = new ProfilingConfig();
            // Add a configuration for each intermediate node (Unaryoperator/BinaryOperator)
            pc.setUdfsComplexity(Arrays.stream(DEFAULT_UDF_COMPLEXITIES.split(",")).map(Integer::valueOf).collect(Collectors.toList()));
            pc.setInputRatio(Arrays.stream(DEFAULT_BINARY_INPUT_RATIOS.split(",")).map(Integer::valueOf).collect(Collectors.toList()));
            pcHashList.put(i,pc);
        }
        return pcHashList;
    }
}
