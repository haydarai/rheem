package org.qcri.rheem.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.qcri.rheem.basic.plugin.RheemBasic;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.ignite.execution.IgniteContextReference;
import org.qcri.rheem.ignite.execution.IgniteExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * {@link Platform} for Apache Ignite.
 */
public class IgnitePlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Ignite";

    private static final String CONFIG_NAME = "ignite";

    private static final String DEFAULT_CONFIG_FILE = "rheem-ignite-defaults.properties";

    public static final String INITIALIZATION_MS_CONFIG_KEY = "rheem.ignite.init.ms";

    private static IgnitePlatform instance = null;

    private static final String[] REQUIRED_IGNITE_PROPERTIES = {
    };

    private static final String[] OPTIONAL_IGNITE_PROPERTIES = {
    };

    /**TODO: see the caracteriscti of the Ignite process
     * <i>Lazy-initialized.</i> Maintains a reference to a {@link Ignite}. This instance's reference, however,
     * does not hold a counted reference, so it might be disposed.
     */
    private IgniteContextReference igniteContextReference = null;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static IgnitePlatform getInstance() {
        if (instance == null) {
            instance = new IgnitePlatform();
        }
        return instance;
    }

    private IgnitePlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    /**TODO: see the caracteriscti of the Ignite process
     * Configures the single maintained {@link Ignite} according to the {@code job} and returns it.
     *
     * @return a {@link IgniteContextReference} wrapping the {@link Ignite}
     */
    public IgniteContextReference getIgniteContext(Job job) {
        Configuration conf = job.getConfiguration();
        /*
        if(this.flinkContextReference == null)
            switch (conf.getStringProperty("rheem.flink.mode.run")) {
                case "local":
                    this.flinkContextReference = new FlinkContextReference(
                            job.getCrossPlatformExecutor(),
                            ExecutionEnvironment.createLocalEnvironment(),
                            (int)conf.getLongProperty("rheem.flink.paralelism")
                    );
                    break;
                case "distribution":
                    String[] jars = getJars(job);
                    this.flinkContextReference = new FlinkContextReference(
                            job.getCrossPlatformExecutor(),
                            ExecutionEnvironment.createRemoteEnvironment(
                                    conf.getStringProperty("rheem.flink.master"),
                                    Integer.parseInt(conf.getStringProperty("rheem.flink.port")),
                                    jars
                            ),
                            (int)conf.getLongProperty("rheem.flink.paralelism")
                    );
                    break;
                case "collection":
                default:
                    this.flinkContextReference = new FlinkContextReference(
                            job.getCrossPlatformExecutor(),
                            new CollectionEnvironment(),
                            1
                    );
                    break;
            }
        return this.flinkContextReference;
        */
        //TODO: Crear las variacion para diferentes tipos de contextos
        this.igniteContextReference = new IgniteContextReference(
                job.getCrossPlatformExecutor(),
                Ignition.ignite(),
                1
        );
        return this.igniteContextReference;

    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new IgniteExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("rheem.ignite.cpu.mhz");
        int numCores = (int) ( configuration.getLongProperty("rheem.ignite.paralelism"));
        double hdfsMsPerMb = configuration.getDoubleProperty("rheem.ignite.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("rheem.ignite.network.ms-per-mb");
        double stretch = configuration.getDoubleProperty("rheem.ignite.stretch");
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(networkMsPerMb / 1000000d),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("rheem.ignite.costs.fix"),
                configuration.getDoubleProperty("rheem.ignite.costs.per-ms")
        );
    }


    private String[] getJars(Job job){
        List<String> jars = new ArrayList<>(5);
        List<Class> clazzs = Arrays.asList(new Class[]{IgnitePlatform.class, RheemBasic.class, RheemContext.class});

        clazzs.stream().map(
                ReflectionUtils::getDeclaringJar
        ).filter(
                element -> element != null
        ).forEach(jars::add);

        final Set<String> udfJarPaths = job.getUdfJarPaths();
        if (udfJarPaths.isEmpty()) {
            this.logger.warn("Non-local IgniteContext but not UDF JARs have been declared.");
        } else {
            udfJarPaths.stream().filter(a -> a != null).forEach(jars::add);
        }

        return jars.toArray(new String[0]);
    }
}
