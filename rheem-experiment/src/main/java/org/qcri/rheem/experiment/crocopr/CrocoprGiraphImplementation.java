package org.qcri.rheem.experiment.crocopr;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.qcri.rheem.experiment.crocopr.udf.giraph.PageRankAlgorithm;
import org.qcri.rheem.experiment.crocopr.udf.giraph.PageRankParameters;
import org.qcri.rheem.experiment.implementations.giraph.GiraphImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.io.IOException;

public class CrocoprGiraphImplementation extends GiraphImplementation {

    public CrocoprGiraphImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }
    @Override
    protected void preExecutePlan() {
        super.preExecutePlan();

        PageRankParameters.setParameter(PageRankParameters.PageRankEnum.ITERATION, 10);

    }
    @Override
    protected void doExecutePlan() {
        try {
            GiraphConfiguration conf = new GiraphConfiguration();
            //vertex reader
            conf.set("giraph.vertex.input.dir", "hdfs://10.4.4.43:8300/data/pages_unredirected/tmp");
            conf.set("mapred.job.tracker", "10.4.4.43:50030");
            conf.set("mapreduce.job.counters.limit", String.valueOf(1));
            conf.setWorkerConfiguration(1,1,100.0f);
            conf.set("giraph.SplitMasterWorker", "false");
            conf.set("mapreduce.output.fileoutputformat.outputdir", "hdfs://10.4.4.43:8300/data/pages_unredirected/result");
            conf.setComputationClass(PageRankAlgorithm.class);
            conf.setVertexInputFormatClass(
                    PageRankAlgorithm.PageRankVertexInputFormat.class);
            conf.setWorkerContextClass(
                    PageRankAlgorithm.PageRankWorkerContext.class);
            conf.setMasterComputeClass(
                    PageRankAlgorithm.PageRankMasterCompute.class);
            conf.setNumComputeThreads(4);

            conf.setVertexOutputFormatClass(PageRankAlgorithm.PageRankVertexOutputFormat.class);

            GiraphJob job = job = new GiraphJob(conf, "rheem-giraph");
            job.run(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
