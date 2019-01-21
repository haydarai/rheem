package org.qcri.rheem.experiment.enviroment;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.experiment.ExperimentExecutor;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class SparkEnviroment extends EnviromentExecution<JavaSparkContext> {
    @Override
    public JavaSparkContext getEnviroment() {
        try {
            ConfigValue prop = ConfigFactory.load().getValue("spark.configuration");
            Config conf = ConfigFactory.parseFile(new File(URI.create((String) prop.unwrapped())));

            SparkConf sparkConf = new SparkConf(true);

            sparkConf.setMaster(conf.getString("spark.master"))
                    .setAppName(conf.getString("spark.app.name"));

            Set<Map.Entry<String, ConfigValue>> entries = conf.entrySet();
            for (Map.Entry<String, ConfigValue> entry : entries) {
                sparkConf.set(entry.getKey(), conf.getString(entry.getKey()));
            }

            String[] jars = new String[]{
                    ReflectionUtils.getDeclaringJar(ExperimentExecutor.class)
            };

            sparkConf.setJars(jars);

            return new JavaSparkContext(sparkConf);
        }catch (ConfigException ex){
            SparkConf sparkConf = new SparkConf();

            sparkConf.setMaster("local[*]")
                    .setAppName("local spark");

            String[] jars = new String[]{
                    ReflectionUtils.getDeclaringJar(ExperimentExecutor.class),
                    ReflectionUtils.getDeclaringJar(org.apache.spark.graphx.Edge.class)
            };

            sparkConf.setJars(jars);

            return new JavaSparkContext(sparkConf);
        }
    }
}
