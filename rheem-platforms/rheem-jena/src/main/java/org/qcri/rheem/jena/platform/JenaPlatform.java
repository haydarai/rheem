package org.qcri.rheem.jena.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.jena.channels.SparqlQueryChannel;
import org.qcri.rheem.jena.execution.JenaExecutor;

public class JenaPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Jena";

    private static final String CONFIG_NAME = "jena";

    private static final String DEFAULT_CONFIG_FILE = "rheem-jena-defaults.properties";

    private static JenaPlatform instance = null;

    public static JenaPlatform getInstance() {
        if (instance == null) {
            instance = new JenaPlatform();
        }
        return instance;
    }

    private JenaPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    protected void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JenaExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        return null;
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return null;
    }

    private final SparqlQueryChannel.Descriptor sparqlQueryChannelDescriptor = new SparqlQueryChannel.Descriptor(this);

    protected JenaPlatform(String platformName, String configName) {
        super(platformName, configName);
    }

    public String getPlatformId() {
        return this.getConfigurationName();
    }

    public SparqlQueryChannel.Descriptor getSparqlQueryChannelDescriptor() {
        return this.sparqlQueryChannelDescriptor;
    }
}
