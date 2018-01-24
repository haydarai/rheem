package org.qcri.rheem.basic.collection;

import org.qcri.rheem.core.api.Configuration;

/**
 * Created by bertty on 12-11-17.
 */
public class MultiplexConfiguration {
    public static long TIME_REVIEW;
    public static long TIME_ASKED;
    public static long TIME_WAIT;


    public static void load(Configuration configuration){
      //  TIME_REVIEW = configuration.getLongProperty("rheem.multiplex.collection.time.review", 1000);
      //  TIME_ASKED  = configuration.getLongProperty("rheem.multiplex.collection.time.asked", 1000);
      //  TIME_WAIT   =configuration.getLongProperty("rheem.multiplex.collection.time.wait", 1000);
        TIME_REVIEW  = 150;
        TIME_ASKED  = 100;
        TIME_WAIT  = 500;


    }


}
