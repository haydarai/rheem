package org.qcri.rheem.jena;

import org.qcri.rheem.jena.platform.JenaPlatform;
import org.qcri.rheem.jena.plugin.JenaConversionsPlugin;
import org.qcri.rheem.jena.plugin.JenaPlugin;

public class Jena {

    private final static JenaPlugin PLUGIN = new JenaPlugin();

    private final static JenaConversionsPlugin CONVERSIONS_PLUGIN = new JenaConversionsPlugin();

    public static JenaPlugin plugin() {
        return PLUGIN;
    }

    public static JenaConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }

    public static JenaPlatform platform() {
        return JenaPlatform.getInstance();
    }
}
