package org.qcri.rheem.profiler.core;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.mathex.model.Constant;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.profiler.core.api.*;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.data.UdfGenerators;
import org.qcri.rheem.profiler.java.JavaSourceProfiler;
import org.qcri.rheem.profiler.spark.SparkOperatorProfiler;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.profiler.util.RrdAccessor;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.rrd4j.ConsolFun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Runs profiling Configuration
 */
public class ProfilingRunner{
    private static final Logger logger = LoggerFactory.getLogger(ProfilingRunner.class);

    PlatformExecution profilingPlatformExecution;
    private static ProfilingConfig profilingConfig;
    ProfilingPlan profilingPlan;
    private static RheemContext rheemContext;
    private static int cpuMhz, numMachines, numCoresPerMachine, numPartitions;
    private static String gangliaRrdsDir;
    private static Configuration configuration = new Configuration();

    public void ProfilingRunner(ProfilingConfig profilingConfig, PlatformExecution profilingPlatformExecution,
                                 ProfilingPlan profilingPlan){
         this.profilingConfig = profilingConfig;
         this.profilingPlatformExecution = profilingPlatformExecution;
         this.profilingPlan = profilingPlan;

        this.cpuMhz = (int) configuration.getLongProperty("rheem.spark.cpu.mhz", 2700);
        this.numMachines = (int) configuration.getLongProperty("rheem.spark.machines", 1);
        this.numCoresPerMachine = (int) configuration.getLongProperty("rheem.spark.cores-per-machine", 1);
        this.numPartitions = (int) configuration.getLongProperty("rheem.spark.partitions", -1);
        gangliaRrdsDir = configuration.getStringProperty("rheem.ganglia.rrds", "/var/lib/ganglia/rrds");

    }

    /*public static void exhaustiveProfiling(List<List<PlanProfiler>> planProfiler,
                                         ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        for (List<PlanProfiler>list:planProfiler)
            list.stream()
                .forEach(plan ->  System.out.println(executePipelineProfiling(plan).toCsvString()));
    }*/

    /**
     * This method will execute the rheem plan associated with the input {@link Shape} with all possible configuration parameters:
     * - input cardinality (for the source operators)
     * // TODO: - input data type
     * - data quanta size
     * - udf complexity for each operator
     * - selectivty complexity
     * @param shapes
     * @param profilingConfiguration
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void exhaustiveProfiling(List<Shape> shapes,
                                           ProfilingConfig profilingConfiguration) {
        profilingConfig = profilingConfiguration;

        shapes.stream().forEach(s -> s.getSubShapes().stream().forEach(executionShape->{
            executeShapeProfiling(executionShape);
            })
        );

    }

    private static List<OperatorProfiler.Result> executeShapeProfiling(Shape shape) {


        //RheemContext rheemContext1 = new RheemContext();
        //rheemContext1.register(Spark.basicPlugin());
        switch (shape.getPlateform()) {
            case "java":
                rheemContext = new RheemContext().with(Java.basicPlugin());
               //rheemContext = new RheemContext().with(Java.basicPlugin());
            case "spark":
                rheemContext = new RheemContext().with(Spark.basicPlugin());
        }

        // Check dataType of the generated plan
        //checkDataType(shape);
        List<OperatorProfiler.Result> results = new ArrayList<>();

        // Loop through dataQuantas cardinality

        // Loop through dataQuanta size

        for (int dataQuantaSize:profilingConfig.getDataQuantaSize()){
            for (long inputCardinality:profilingConfig.getInputCardinality()){

                System.out.printf("[PROFILING] Running Synthetic Plan with %d data quanta cardinality, %d data quanta size of %s on %s platform ;" +
                                " with  %d Topology Number;  %d Pipeline Topollogies; %d Juncture Topologies;" +
                                " %d Loop Topologies  \n",
                        inputCardinality,dataQuantaSize,
                        shape.getSourceTopologies().get(0).getNodes().get(0).getField1().getOperator().getOutput(0).getType().toString(),
                        shape.getPlateform(),shape.getTopologyNumber(),shape.getPipelineTopologies().size(),shape.getJunctureTopologies().size(), shape.getLoopTopologies().size());
                //shape.getAllTopologies().stream().forEach(t->t.getNodeNumber())

                // Clear the garbage collector
                System.gc();

                // Prepare input source operator
                for (Topology t:shape.getSourceTopologies()){
                    switch (shape.getPlateform()){
                        case "java":
                            JavaSourceProfiler sourceProfiler = (JavaSourceProfiler) t.getNodes().firstElement().getField1();

                            // Update the dataQuantumGenerators with the appropriate dataQuanta size
                            sourceProfiler.setDataQuantumGenerators(DataGenerators.generateGenerator(dataQuantaSize,
                                    sourceProfiler.getOperator().getOutput(0).getType()));
                            try {
                                // Prepare source operator
                                sourceProfiler.setUpSourceData(inputCardinality);
                            } catch (Exception e) {
                                LoggerFactory.getLogger(ProfilingRunner.class).error(
                                        String.format("Failed to set up source data for input cardinality %d.", inputCardinality),
                                        e
                                );
                            }
                            break;
                        case "spark":
                            SparkOperatorProfiler sparkSourceProfiler = (SparkOperatorProfiler) t.getNodes().firstElement().getField1();

                            // Update the dataQuantumGenerators with the appropriate dataQuanta size
                            sparkSourceProfiler.setDataQuantumGenerators(DataGenerators.generateGenerator(dataQuantaSize,
                                    sparkSourceProfiler.getOperator().getOutput(0).getType()));

                            // Prepare source operator
                            sparkSourceProfiler.prepare(dataQuantaSize,inputCardinality);
                            break;
                    }

                }

                // save the starting execution time of current {@link RheemPlan}
                final long startTime = System.currentTimeMillis();

                Topology sinkTopology = shape.getSinkTopology();
                ExecutionOperator sinkOperator = sinkTopology.getNodes().elementAt(sinkTopology.getNodes().size()-1).getField1().getOperator();

                // Have Rheem execute the plan.
                Job job = rheemContext.createJob(null, new RheemPlan(sinkOperator));

                // Add jars to spark workers
                job.addUdfJar(ReflectionUtils.getDeclaringJar(UdfGenerators.class));

                try {
                    job.execute();
                }catch (Exception e){
                    System.out.print("[ERROR] Job aborted! \n");
                    System.out.print(e.getMessage()+"\n");
                }
                final long endTime = System.currentTimeMillis();

                // Refresh the input cardinality and DAtaQuantaSize for logging
                int[] vectorLogs = shape.getVectorLogs();
                vectorLogs[103] = (int) inputCardinality;
                vectorLogs[104] =  dataQuantaSize;

                logExecution(shape,endTime - startTime);

                List<Long> inputCardinalities = new ArrayList<>();
                //inputCardinalities.add((long) shape.getSourceTopologies().get(0).getNodes().elementAt(0).getField1().getOperator().getNumOutputs());
                // Gather and assemble all result metrics.
                results.add( new OperatorProfiler.Result(
                        inputCardinalities,
                        1,
//                (long)  shape.getSourceTopologies().get(0).getNodes().elementAt(0).getField1().getOperator().getNumInputs(),
                        endTime - startTime,
                        provideDiskBytes(startTime, endTime),
                        provideNetworkBytes(startTime, endTime),
                        provideCpuCycles(startTime, endTime),
                        numMachines,
                        numCoresPerMachine
                    )
                );
            }
        }
        return results;

    }


    /**
     * Generate the log for training the ML for learning Topology models
     */
    private static void logExecution(Shape shape, long executionTime){
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            executionLog.storeVector(shape.getVectorLogs(),executionTime);
        } catch (Exception e) {
            logger.error("Storing partial executions failed.", e);
        }
    }
    public static void exhaustivePlanProfiling(List<List<PlanProfiler>> planProfiler,
                                           ProfilingConfig profilingConfiguration){
        profilingConfig = profilingConfiguration;
        for (List<PlanProfiler>list:planProfiler)
            list.stream()
                    .forEach(plan ->  System.out.println(executePipelineProfiling(plan).toCsvString()));
    }

    public static void pipelineProfiling(List<PlanProfiler> planProfiler,
                                                          ProfilingConfig profilingConfiguration){
         profilingConfig = profilingConfiguration;
         planProfiler.stream()
                 .forEach(plan ->  System.out.println(executePipelineProfiling(plan).toCsvString()));
    }

    public static void preparePipelineProfiling(PlanProfiler plan){
        switch (profilingConfig.getProfilingPlateform().get(0)){
            case "java":
                rheemContext = new RheemContext().with(Java.basicPlugin());
            case "spark":
                rheemContext = new RheemContext().with(Spark.basicPlugin());
        }
        plan.unaryOperatorProfilers.get(0).getOperator().connectTo(0,plan.sinkOperatorProfiler.getOperator(),0);

        plan.getSourceOperatorProfiler().getOperator().connectTo(0,plan.unaryOperatorProfilers.get(0).getOperator(),0);

    }

    private static OperatorProfiler.Result executePipelineProfiling(PlanProfiler plan){

        preparePipelineProfiling(plan);
        List<Integer> results = new ArrayList<>();

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(results, Integer.class);

        //plan.sinkOperatorProfiler.getOperator().connectTo(0,sink,0);

        final long startTime = System.currentTimeMillis();

        // Have Rheem execute the plan.
        rheemContext.execute(new RheemPlan(plan.sinkOperatorProfiler.getOperator()));

        final long endTime = System.currentTimeMillis();

        List<Long> inputCardinalities = new ArrayList<>();
        inputCardinalities.add((long) plan.getSourceOperatorProfiler().getOperator().getNumOutputs());
        // Gather and assemble all result metrics.
        return new OperatorProfiler.Result(
                inputCardinalities,
                (long) plan.getSourceOperatorProfiler().getOperator().getNumInputs(),
                endTime - startTime,
                provideDiskBytes(startTime, endTime),
                provideNetworkBytes(startTime, endTime),
                provideCpuCycles(startTime, endTime),
                numMachines,
                numCoresPerMachine
        );

    }

    /**
     * Profiling single operator
     * @param operatorsProfiler
     * @param profilingConfig
     * @return
     */
    public static List<OperatorProfiler.Result> SingleOperatorProfiling(List<? extends OperatorProfiler> operatorsProfiler,
                                                          ProfilingConfig profilingConfig){
        // Set the configuration parameters
        List<Long> inputCardinality = profilingConfig.getInputCardinality();
        List<Integer> dataQuantas = profilingConfig.getDataQuantaSize() ;
        List<Integer> UdfsComplexity = profilingConfig.getUdfsComplexity();
        List<Integer> inputRatio = profilingConfig.getInputRatio();

        // Profiling results
        List<OperatorProfiler.Result> allOperatorsResult = new ArrayList<>();

        for(OperatorProfiler operatorProfiler:operatorsProfiler){
             List<OperatorProfiler.Result> operatorResult = new ArrayList<>();
             System.out.println("*****************************************************");
             System.out.println("Starting profiling of " + operatorProfiler.getOperator().getName() + " operator: ");
             for (long cardinality:inputCardinality){
                 for (int dataQanta:dataQuantas){
                     for (int udf:UdfsComplexity){
                         for(int inRatio:inputRatio){
                             // configure profiling
                             //operatorProfiler.set
                             // Do profiling
                             System.out.printf("Profiling %s with %s data quanta.\n", operatorProfiler, RheemArrays.asList(cardinality));
                             final StopWatch stopWatch = createStopWatch();
                             OperatorProfiler.Result result = null;
                             OperatorProfiler.Result averageResult = null;
                             List<OperatorProfiler.Result> threeRunsResult = new ArrayList<>();

                             try {
                                 // Execute 3 runs
                                 for(int i=1;i<=3;i++){
                                     System.out.println("Prepare Run"+i+"...");
                                     final TimeMeasurement preparation = stopWatch.start("Preparation");
                                     SparkPlatform.getInstance().warmUp(new Configuration());
                                     operatorProfiler.prepare(dataQanta,cardinality);
                                     preparation.stop();


                                     // Execute 3 runs
                                     //for(int i=1;i<=3;i++){
                                     System.out.println("Execute Run"+i+"...");
                                     final TimeMeasurement execution = stopWatch.start("Execution");
                                     result = operatorProfiler.run();
                                     threeRunsResult.add(result);
                                     execution.stop();

                                     System.out.println("Meas urement Run "+i+":");
                                     if (result != null) System.out.println(result);
                                     System.out.println(stopWatch.toPrettyString());
                                     System.out.println();
                                 }
                                 averageResult = OperatorProfiler.averageResult(threeRunsResult);
                             } finally {
                                 System.out.println("Clean up...");
                                 final TimeMeasurement cleanUp = stopWatch.start("Clean up");
                                 operatorProfiler.cleanUp();
                                 cleanUp.stop();

                                 System.out.println("Average Measurement:");
                                 if (result != null) System.out.println(averageResult);
                                 System.out.println(stopWatch.toPrettyString());
                                 System.out.println();
                             }

                             operatorResult.add(averageResult);
                             System.out.println("# Intermidiate results");
                             System.out.println(RheemCollections.getAny(operatorResult).getCsvHeader());
                             operatorResult.forEach(r -> System.out.println(r.toCsvString()));

                         }

                     }
                 }
             }
             allOperatorsResult.addAll(operatorResult);
         }
         return allOperatorsResult;
    }

    private static StopWatch createStopWatch() {
        Experiment experiment = new Experiment("rheem-profiler", new Subject("Rheem", "0.1"));
        return new StopWatch(experiment);
    }


    /**
     * Estimates the disk bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    private static long provideCpuCycles(long startTime, long endTime) {
        // Find out the average idle fraction in the cluster.
        final double sumCpuIdleRatio = waitAndQueryMetricAverage("cpu_idle", "sum", startTime, endTime);
        final double numCpuIdleRatio = waitAndQueryMetricAverage("cpu_idle", "num", startTime, endTime);
        final double avgCpuIdleRatio = sumCpuIdleRatio / numCpuIdleRatio / 100;

        // Determine number of cycles per millisecond.
        long passedMillis = endTime - startTime;
        double cyclesPerMillis = cpuMhz * 1e3 * numCoresPerMachine * numMachines;

        // Estimate the number of spent CPU cycles in the cluster.
        return Math.round(passedMillis * cyclesPerMillis * (1 - avgCpuIdleRatio));
    }

    /**
     * Estimates the network bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    protected static long provideNetworkBytes(long startTime, long endTime) {
        // Find out the average received/transmitted bytes per second.
        final double transmittedBytesPerSec = waitAndQueryMetricAverage("bytes_out", "sum", startTime, endTime);
        final double receivedBytesPerSec = waitAndQueryMetricAverage("bytes_in", "sum", startTime, endTime);
        final double bytesPerSec = (transmittedBytesPerSec + receivedBytesPerSec) / 2;

        // Estimate the number of actually communicated bytes.
        return (long) (bytesPerSec / 1000 * (endTime - startTime));

    }

    /**
     * Estimates the disk bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    protected static long provideDiskBytes(long startTime, long endTime) {
        // Find out the average received/transmitted bytes per second.
        final double readBytesPerSec = waitAndQueryMetricAverage("diskstat_sdb1_read_bytes_per_sec", "sum", startTime, endTime);
        final double writeBytesPerSec = waitAndQueryMetricAverage("diskstat_sdb1_write_bytes_per_sec", "sum", startTime, endTime);
        final double bytesPerSec = readBytesPerSec + writeBytesPerSec;

        // Estimate the number of actually communicated bytes.
        return (long) (bytesPerSec / 1000 * (endTime - startTime));
    }

    /**
     * Queries an average metric from a Ganglia RRD file. If the metric is not recent enough, this method waits
     * until the requested data points are available.
     */
    static class test{

    }
    private static double waitAndQueryMetricAverage(String metric, String dataSeries, long startTime, long endTime) {
        /*final String rrdFile = this.gangliaRrdsDir + File.separator +
                this.gangliaClusterName + File.separator +
                "__SummaryInfo__" + File.separator +
                metric + ".rrd";*/
        //Logger logger = new LoggerFactory(ProfilingRunner.test.class);
        final String rrdFile = gangliaRrdsDir + File.separator +
                "__SummaryInfo__" + File.separator +
                metric + ".rrd";
        //final String rrdFile = "/tmp";
        double metricValue = Double.NaN;
        int numAttempts = 0;
        do {
            if (numAttempts++ > 0) {
                ProfilingUtils.sleep(5000);
            }

            try (RrdAccessor rrdAccessor = RrdAccessor.open(rrdFile)) {
                final long lastUpdateMillis = rrdAccessor.getLastUpdateMillis();
                if (lastUpdateMillis >= endTime) {
                    metricValue = rrdAccessor.query(dataSeries, startTime, endTime, ConsolFun.AVERAGE);
                } else {
                    //logger.info("Last RRD file update is only from {} ({} attempts so far).", new Date(lastUpdateMillis), numAttempts);
                }
            } catch (Exception e) {
                //logger.error(String.format("Could not access RRD %s.", rrdFile), e);
                return Double.NaN;
            }
        } while (Double.isNaN(metricValue));

        return metricValue;
    }

}
