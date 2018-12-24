package org.qcri.rheem.core.optimizer.mloptimizer;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Load saved learned model
 */
public class LoadModel {
    public static final URI MODEL_LOADING_LOCATION = createUri("/loadModel.py");

    private static final Logger logger = LoggerFactory.getLogger(LoadModel.class);

    private static Configuration configuration = new Configuration();

    /**
     * Created on demand an can be closed as well.
     */
    private static BufferedWriter writer;

    private static List<double[]> featureVectors = new ArrayList<>();

    /**
     * Run "loadModel.py"
     * @param featurevectors
     */
    public static void loadModel(List<double[]> featurevectors) {

        featureVectors = featurevectors;

        System.out.println(MODEL_LOADING_LOCATION);
        // Check if `loadModel.py` is under .Rheem directory
        File loadModelFile  = new File(MODEL_LOADING_LOCATION);


        // remove a previously stored on disk vector
        File savedVectors = new File(configuration.getStringProperty("rheem.core.optimizer.mloptimizer.saveVectorLocation"));
        if(savedVectors.exists())
            savedVectors.delete();

        // Store plan vector
        storeVector(featureVectors,0);

        //build command to run M-Learned model
        String[] cmd = {
                configuration.getStringProperty("rheem.core.optimizer.mloptimizer.loadModelEnvironment.python.path","python")
                , String.valueOf(Paths.get(MODEL_LOADING_LOCATION))
                , configuration.getStringProperty("rheem.core.optimizer.mloptimizer.model")
        };
        try {
            // Run M-learned model
            Process p = Runtime.getRuntime().exec(cmd);

            // Check if there is no error when running python script
            BufferedReader brError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            Stream<String> errorStream = brError.lines();
            errorStream.forEach(s->{
                logger.error(s);
                if (s.contains("Error"))
                    throw new RheemException(String.format("Error when running python script `loadModel.py`.\n[ERROR] %s", s));
            });
            // Destroy process after finish execution
            p.waitFor();
            p.destroy();
        } catch (IOException e) {
            // get error stack
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            // Throw rheem exception
            throw new RheemException(String.format("Could not run M-Learned model command.\n %s", sw.toString()));
        } catch (InterruptedException e) {
            throw new RheemException("ML model estimation took too long..");
        }
    }

    /**
     * Store the vector on disk so to be used as input by ML model
     * @param featureVectors
     * @param executionTime
     */
    private static void storeVector(List<double[]> featureVectors, long executionTime) {

        try {
            File file = new File(configuration.getStringProperty("rheem.core.optimizer.mloptimizer.saveVectorLocation"));
            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new RheemException("Could not initialize cardinality repository.");
            }

            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));

//            // create 2Dlog file directory
//            File file2dLog = new File(configuration.getStringProperty("rheem.core.log.2Dlogs"));
//            //file2dLog.mkdir();
//            if (!parentFile.exists() && !file2dLog.getParentFile().mkdirs()) {
//                throw new RheemException("Could not initialize 2d log repository.");
//            }
        } catch (RheemException e) {
            throw e;
        } catch (Exception e) {
            throw new RheemException(String.format("Cannot write to %s.",
                    configuration.getStringProperty("rheem.core.optimizer.mloptimizer.saveVectorLocation")), e);
        }

        // Handle 1D log storage
        NumberFormat nf = new DecimalFormat("##.#");
        try {
            for(double[] logs:featureVectors){
                for(int i=0;i<logs.length;i++){
                    writer.write( nf.format( logs[i]) + " ");
                }
                writer.write(Long.toString(0));
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            throw new RheemException("could not stream vectors to ML model!");
        }
    }

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }
    public static void main(String[] args){
        String[] cmd = {
                "python",configuration.getStringProperty("rheem.core.optimizer.mloptimizer.modelLocation")
        };

        //String cmd = "/bash/bin -c echo password| python script.py '" + packet.toString() + "'";

        try {
            Process p = Runtime.getRuntime().exec(cmd);

            //Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String s = br.readLine();
            System.out.println(s);
            System.out.println("Sent");
            p.waitFor();
            p.destroy();
        } catch (IOException e) {
            throw new RheemException("could not load properly the ML model!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
