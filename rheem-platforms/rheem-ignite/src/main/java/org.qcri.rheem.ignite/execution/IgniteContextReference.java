package org.qcri.rheem.ignite.execution;

import org.apache.ignite.Ignite;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionResourceTemplate;

/**
 * Wraps and manages a Ignite {@link Ignite} to avoid steady re-creation.
 */
public class IgniteContextReference extends ExecutionResourceTemplate {
    /**
     * The wrapped {@link Ignite}.
     */
    private Ignite igniteEnviroment;

    /**
     * Creates a new instance.
     *
     * @param igniteEnviroment the {@link Ignite} to be wrapped
     */
    public IgniteContextReference(CrossPlatformExecutor crossPlatformExecutor, Ignite igniteEnviroment, int parallelism) {
        super(null);
        if (crossPlatformExecutor != null) {
            crossPlatformExecutor.registerGlobal(this);
        }
        this.igniteEnviroment = igniteEnviroment;
    }


    /**
     * Provides the {@link Ignite}. This instance must not be disposed, yet.
     *
     * @return the wrapped {@link Ignite}
     */
    public Ignite getIgniteEnviroment() {
        return this.igniteEnviroment;
    }

    @Override
    protected void doDispose() throws Throwable {
        this.igniteEnviroment.close();
    }

    @Override
    public boolean isDisposed() {
        return true;
    }
}
