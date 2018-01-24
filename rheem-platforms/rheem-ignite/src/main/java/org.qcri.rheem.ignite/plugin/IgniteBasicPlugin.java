package org.qcri.rheem.ignite.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.ignite.channels.ChannelConversions;
import org.qcri.rheem.ignite.mapping.Mappings;
import org.qcri.rheem.ignite.platform.IgnitePlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Plugin} enables to use the basic Rheem {@link Operator}s on the {@link IgnitePlatform}.
 */
public class IgniteBasicPlugin implements Plugin {
    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(IgnitePlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.BASIC_MAPPINGS;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {

    }
}
