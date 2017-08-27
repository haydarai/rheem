package org.qcri.rheem.profiler.core;

import org.qcri.rheem.profiler.core.api.ProfilingConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 * Generates profiling configuration for a profiling plan
 */
public class ProfilingConfigurer {

    //private static final String DEFAULT_INPUT_CARDINALITIES = "1,100,1000,10000,100000,1000000,10000000,20000000";
    private static final String DEFAULT_INPUT_CARDINALITIES = "1";

    //private static final String DEFAULT_DATA_QUATA_SIZES = "1,10,100,1000,5000,10000";

    private static final String DEFAULT_DATA_QUATA_SIZES = "1";

    // TODO: replace with actual read functions from a user input file
    private static final String DEFAULT_UDF_COMPLEXITIES = "1,2,3";

    private static final String DEFAULT_SELECTIVITY_COMPLEXITIES = "1,2,3";

    // The below can be applied only for unary operator profiling
    private static final String DEFAULT_BINARY_INPUT_RATIOS = "1,10,100";

    private static final String DEFAULT_PLATEFORM = "java";

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
        pc.setProfilingPlateform("java");
        pc.setBushyGeneration(DEFAULT_BUSHY_GENERATION);
        pc.setProfilingPlanGenerationEnumeration("exhaustive");
        pc.setProfilingConfigurationEnumeration("exhaustive");
        pc.setInputCardinality(inputCardinality);
        pc.setDataQuantaSize(dataQuantas);
        pc.setUdfsComplexity(UdfsComplexity);
        pc.setInputRatio(inputRatio);
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
