package org.qcri.rheem.core.optimizer.mloptimizer;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Load saved learned model
 */
public class LoadModel {
    private static final Logger logger = LoggerFactory.getLogger(LoadModel.class);

    private static Configuration configuration = new Configuration();

    /**
     * Created on demand an can be closed as well.
     */
    private static BufferedWriter writer;

    private static List<double[]> featureVectors = new ArrayList<>();

    public static void loadModel(List<double[]> featurevectors) {

        featureVectors = featurevectors;

        // check if feature file exists
        File file = new File(configuration.getStringProperty("rheem.core.optimizer.mloptimizer.saveVectorLocation"));
        file.delete();

        storeVector(featureVectors,0);

//        // save features vectors
//        featureVectors.stream()
//                .forEach(f -> {
//                    storeVector(f, 0);
//                });

        // run model loader in python

        String[] cmd = {
                "python",configuration.getStringProperty("rheem.core.optimizer.mloptimizer.modelLocation")
        };

        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String s = br.readLine();
            logger.info(s);
            System.out.println(s);
            p.waitFor();
            p.destroy();
        } catch (IOException e) {
            throw new RheemException("could not load properly the ML model!");
        } catch (InterruptedException e) {
            throw new RheemException("could not wait for ML model to finish estimation!");
        }

    }

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
            throw new RheemException(String.format("Cannot write to %s.", configuration.getStringProperty("rheem.core.optimizer.mloptimizer.saveVectorLocation")), e);
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

//     public static void main(String[] args) throws FileNotFoundException, JAXBException, SAXException {
//        loadLodel();
//    }
//
//    private static void loadLodel() throws FileNotFoundException, JAXBException, SAXException {
//
//        PMML pmml = null;
//
//
//        try(InputStream inputstream = new FileInputStream("C:\\Users\\FLVBSLV\\Documents\\GitHub\\jpmml-sklearn\\pipeline.pmml")){
//            pmml = org.jpmml.model.PMMLUtil.unmarshal(inputstream);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
//
//        ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
//
//        Evaluator evaluator = (Evaluator)modelEvaluator;
//
//        List<InputField> inputFields = evaluator.getInputFields();
//        System.out.println(inputFields);
//    }
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
