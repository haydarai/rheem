package org.qcri.rheem.experiment;

import org.apache.commons.cli.*;

import java.net.URI;

public abstract class ExperimentController {

    protected String[] args;
    private Options options;
    private CommandLine cmd;

    public ExperimentController(String... args){
        this.args = args;
        this.options = buildOptions();
        this.generateCommandLine();
    }

    protected abstract Options buildOptions();

    protected void generateCommandLine(){
        String[] args = getArgs();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        this.cmd = null;


        try {
            this.cmd = parser.parse(options, args, true);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        String validation_message = isValid();
        if( validation_message != null){
            throw new ExperimentException(validation_message);
        }
    }

    protected abstract String isValid();

    protected void setArgs(String... args){
        this.args = args;
    }

    protected String[] getArgs(){
        return this.args;
    }

    public String getValue(String key){
        return this.cmd.getOptionValue(key);
    }

    public String[] getValues(String key){
        return this.cmd.getOptionValues(key);
    }

    public int getIntValue(String key){
        return Integer.parseInt(this.getValue(key));
    }

    public int[] getIntValues(String key){
        String[] sValue = this.getValues(key);
        int[] iValue = new int[sValue.length];
        for(int i = 0; i < iValue.length; i++){
            iValue[i] = Integer.parseInt(sValue[i]);
        }
        return iValue;
    }

    public long getLongValue(String key){
        return Long.parseLong(this.getValue(key));
    }

    public long[] getLongValues(String key){
        String[] sValue = this.getValues(key);
        long[] lValue = new long[sValue.length];
        for(int i = 0; i < lValue.length; i++){
            lValue[i] = Long.parseLong(sValue[i]);
        }
        return lValue;
    }

    public float getFloatValue(String key){
        return Float.parseFloat(this.getValue(key));
    }

    public float[] getFloatValues(String key){
        String[] sValue = this.getValues(key);
        float[] fValue = new float[sValue.length];
        for(int i = 0; i < fValue.length; i++){
            fValue[i] = Float.parseFloat(sValue[i]);
        }
        return fValue;
    }

    public double getDoubleValue(String key){
        return Double.parseDouble(this.getValue(key));
    }

    public double[] getDoubleValues(String key){
        String[] sValue = this.getValues(key);
        double[] dValue = new double[sValue.length];
        for(int i = 0; i < dValue.length; i++){
            dValue[i] = Double.parseDouble(sValue[i]);
        }
        return dValue;
    }

    public boolean getBooleanValue(String key){
        return Boolean.getBoolean(this.getValue(key));
    }

    public boolean[] getBooleanValues(String key){
        String[] sValue = this.getValues(key);
        boolean[] bValue = new boolean[sValue.length];
        for(int i = 0; i < bValue.length; i++){
            bValue[i] = Boolean.getBoolean(sValue[i]);
        }
        return bValue;
    }

    public URI getURIValue(String key){
        return URI.create(this.getValue(key));
    }

    public boolean existKey(String key){
        return this.cmd.hasOption(key) && this.getValue(key) != null;
    }

    public String getValue(String key, String default_value){
        return (this.cmd.getOptionValue(key) == null)? default_value: this.cmd.getOptionValue(key);
    }

    public String[] getValues(String key, String[] default_value){
        return (this.cmd.getOptionValues(key) == null)? default_value: this.cmd.getOptionValues(key);
    }

    public int getIntValue(String key, int default_value){
        return Integer.parseInt(this.getValue(key, Integer.toString(default_value)));
    }

    public int[] getIntValues(String key, int[] default_value){
        //TODO Convert this in default mode and all arrays
        return default_value;
       /* String[] sValue = this.getValues(key);
        int[] iValue = new int[sValue.length];
        for(int i = 0; i < iValue.length; i++){
            iValue[i] = Integer.parseInt(sValue[i]);
        }
        return iValue;*/
    }

    public long getLongValue(String key, long default_value){
        return Long.parseLong(this.getValue(key, Long.toString(default_value)));
    }

    public long[] getLongValues(String key, long[] default_value){
        /*String[] sValue = this.getValues(key);
        long[] lValue = new long[sValue.length];
        for(int i = 0; i < lValue.length; i++){
            lValue[i] = Long.parseLong(sValue[i]);
        }
        return lValue;*/
        return default_value;
    }

    public float getFloatValue(String key, float default_value){
        return Float.parseFloat(this.getValue(key, Float.toString(default_value)));
    }

    public float[] getFloatValues(String key, float[] default_value){
        /*String[] sValue = this.getValues(key);
        float[] fValue = new float[sValue.length];
        for(int i = 0; i < fValue.length; i++){
            fValue[i] = Float.parseFloat(sValue[i]);
        }
        return fValue;*/
        return default_value;
    }

    public double getDoubleValue(String key, double default_value){
        return Double.parseDouble(this.getValue(key, Double.toString(default_value)));
    }

    public double[] getDoubleValues(String key, double[] default_value){
        /*String[] sValue = this.getValues(key);
        double[] dValue = new double[sValue.length];
        for(int i = 0; i < dValue.length; i++){
            dValue[i] = Double.parseDouble(sValue[i]);
        }
        return dValue;*/
        return default_value;
    }

    public boolean getBooleanValue(String key, boolean default_value){
        return Boolean.getBoolean(this.getValue(key, Boolean.toString(default_value)));
    }

    public boolean[] getBooleanValues(String key, boolean[] default_value){
        /*String[] sValue = this.getValues(key);
        boolean[] bValue = new boolean[sValue.length];
        for(int i = 0; i < bValue.length; i++){
            bValue[i] = Boolean.getBoolean(sValue[i]);
        }
        return bValue;*/
        return default_value;
    }

    public URI getURIValue(String key, URI default_value){
        return URI.create(this.getValue(key, default_value.toString()));
    }

}
