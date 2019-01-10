package org.qcri.rheem.experiment.enviroment;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

public class RheemEnviroment extends EnviromentExecution<RheemContext> {
    @Override
    public RheemContext getEnviroment() {
        RheemContext rheemContext = new RheemContext();
        String platforms = "java,spark,flink";

        for (String platform : platforms.split(",")) {
            switch (platform) {
                case "java":
                    rheemContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    rheemContext.register(Spark.basicPlugin());
                    break;
                case "flink":
                    rheemContext.register(Flink.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
            }
        }
        return rheemContext;
    }
}
