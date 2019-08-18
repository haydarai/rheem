package org.qcri.rheem.tests.snifferBenchmark;

import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.tests.snifferBenchmark.grep.GrepPlan;
import org.qcri.rheem.tests.snifferBenchmark.join.JoinPlan;
import org.qcri.rheem.tests.snifferBenchmark.join.JoinPlanShort;
import org.qcri.rheem.tests.snifferBenchmark.sintetic.SyntheticPlan;
import org.qcri.rheem.tests.snifferBenchmark.wordcount.WordCountBase;
import org.qcri.rheem.tests.snifferBenchmark.wordcount.WordCountSpecial;

import java.io.IOException;
import java.util.Arrays;

public class    FirstTest {

    public static void main(String ... args){
        String input = args[0];
        String output = args[1];
        String type = args[2];
        int n_sniffers = Integer.parseInt(args[3]);

        System.out.println(Arrays.toString(args));

        SnifferBenchmarkBase bench = null;
        if(type.compareToIgnoreCase("wordcount") == 0) {
            bench = new WordCountBase(n_sniffers, input, output);
        }else if(type.compareToIgnoreCase("special") ==0){
            bench = new WordCountSpecial(n_sniffers, input, output);
        }else if(type.compareToIgnoreCase("synthetic") ==0){
            bench = new SyntheticPlan(34, n_sniffers, input, output);
        }else if(type.compareToIgnoreCase("join") ==0){
            bench = new JoinPlan(n_sniffers, input, output);
        }else if(type.compareToIgnoreCase("joinshort") ==0){
            bench = new JoinPlanShort(n_sniffers, input, output);
        }else if(type.compareToIgnoreCase("grep") ==0){
            bench = new GrepPlan(n_sniffers, input, output);
        }else{
            throw new RuntimeException("The option is not valid");
        }
        try{
            FileSystems.getFileSystem(output).get().delete(output, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        bench.makeOperators(n_sniffers);
        bench.execute();
    }

}
