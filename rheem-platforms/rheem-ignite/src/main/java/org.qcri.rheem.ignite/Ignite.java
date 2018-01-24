package org.qcri.rheem.ignite;

import org.qcri.rheem.ignite.platform.IgnitePlatform;
import org.qcri.rheem.ignite.plugin.IgniteBasicPlugin;
import org.qcri.rheem.ignite.plugin.IgniteConversionPlugin;

/**
 * Register for relevant components of this module.
 */
public class Ignite {
    private final static IgniteBasicPlugin PLUGIN = new IgniteBasicPlugin();

    private final static IgniteConversionPlugin CONVERSION_PLUGIN = new IgniteConversionPlugin();

    /**
     * Retrieve the {@link IgniteBasicPlugin}.
     *
     * @return the {@link IgniteBasicPlugin}
     */
    public static IgniteBasicPlugin basicPlugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link IgniteConversionPlugin}.
     *
     * @return the {@link IgniteConversionPlugin}
     */
    public static IgniteConversionPlugin conversionPlugin() {
        return CONVERSION_PLUGIN;
    }

    /**
     * Retrieve the {@link IgnitePlatform}.
     *
     * @return the {@link IgnitePlatform}
     */
    public static IgnitePlatform platform() {
        return IgnitePlatform.getInstance();
    }

}
