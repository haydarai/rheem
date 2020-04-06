package org.qcri.rheem.jena.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.jena.channels.ChannelConversions;
import org.qcri.rheem.jena.mapping.Mappings;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class JenaPlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(JenaPlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {

    }
}
