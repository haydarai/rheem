package org.qcri.rheem.ignite.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.ignite.channels.ChannelConversions;
import org.qcri.rheem.ignite.platform.IgnitePlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link IgnitePlatform}.
 */
public class IgniteConversionPlugin implements Plugin {
    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(IgnitePlatform.getInstance());
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }
}
