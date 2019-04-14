package org.qcri.rheem.generator;

import de.hpi.isg.profiledb.store.model.Experiment;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.generator.plan.Wordcount;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.apps.simwords.Word2NVec;

import java.util.Arrays;

public class RheemPlanGenerator {


    public static void main(String... args){
        String plan_name = args[0];//Name of the plan
        Platform[] platforms = RheemPlanGenerator.getPlatforms(args[1]);
        int size = Integer.parseInt(args[2]);

        RheemGenerator gen = new RheemGenerator(platforms);
        RheemPlan plan = null;

        switch (plan_name){
            case "wordcount":
                plan = RheemPlanGenerator.wordcount(Arrays.copyOfRange(args, 3, args.length));
                break;
            case "word2nvec":
                plan = RheemPlanGenerator.word2nvec(Arrays.copyOfRange(args, 3, args.length));
                break;
            default:
                System.out.println("the plan is not valid");
                break;
        }

        if(plan == null){
            System.exit(-1);
        }

        long start = System.currentTimeMillis();
        gen.generateAndSave(plan, size);

        long finish = System.currentTimeMillis() - start;
        System.out.println("the time of exection was: "+finish+"ms");
    }

    private static Platform[] getPlatforms(String platforms_str){
        //TODO validate the platforms are unique and more...
        String[] platforms_vec = platforms_str.split(",");
        Platform[] platforms = new Platform[platforms_vec.length];
        for(int i = 0; i < platforms_vec.length; i ++){
            if(platforms_vec[i].equalsIgnoreCase("spark")){
                platforms[i] = Spark.platform();
                continue;
            }
            if(platforms_vec[i].equalsIgnoreCase("flink")){
                platforms[i] = Flink.platform();
                continue;
            }
            if(platforms_vec[i].equalsIgnoreCase("java")){
                platforms[i] = Java.platform();
                continue;
            }
        }
        return platforms;
    }

    private static RheemPlan wordcount(String[] parameters){
        return Wordcount.createRheemPlan(parameters[0], parameters[1]);
    }

    private static RheemPlan word2nvec(String[] parameters){
        Configuration configuration = new Configuration();
        Experiment exp = new Experiment("", null);
        scala.collection.JavaConverters.asScalaIteratorConverter(Arrays.asList(Java.basicPlugin()).iterator()).asScala().toSeq();
        Word2NVec tmp = new Word2NVec(
                            scala.collection
                                    .JavaConverters
                                    .asScalaIteratorConverter(
                                            Arrays.asList(
                                                    (Plugin)Java.basicPlugin(),
                                                    (Plugin)Spark.basicPlugin(),
                                                    (Plugin)Flink.basicPlugin()
                                            ).iterator()
                                    )
                                    .asScala()
                                    .toSeq()
                        );
        return tmp.apply(parameters[0],
                Integer.parseInt(parameters[1]),
                Integer.parseInt(parameters[2]),
                new ProbabilisticDoubleInterval(100, 10000, 0.9),
                null,
                exp,
                configuration);
    }
}
