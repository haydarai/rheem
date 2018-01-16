package org.qcri.rheem.core.optimizer.mloptimizer;

import org.qcri.rheem.core.api.Configuration;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Compute feature vectors estimates
 */

public class MLestimation {

    private static Configuration configuration = new Configuration();
    private static List<Double>  estimates = new ArrayList<>();

    public static double[] getBestVector(List<double[]> featureVectors){

        // Load output estimates
        retreiveEstimates();

        // pick best (minimum) estimate
        int minIndex;

        final ListIterator<Double> itr = estimates.listIterator();
        Double min = itr.next(); // first element as the current minimum
        minIndex = itr.previousIndex();
        while (itr.hasNext()) {
            final Double curr = itr.next();
            if (curr.compareTo(min) < 0) {
                min = curr;
                minIndex = itr.previousIndex();
            }
        }
        return featureVectors.get(minIndex);
    }

    // Load estimated times
    public static void retreiveEstimates() {

        // The name of the file to open.
        String fileName = configuration.getStringProperty("rheem.core.optimizer.mloptimizer.loadEstimatesLocation");

        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                estimates.add(Double.valueOf(line));
                System.out.println(line);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
    }
}
