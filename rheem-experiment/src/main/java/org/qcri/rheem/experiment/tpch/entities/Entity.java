package org.qcri.rheem.experiment.tpch.entities;

import org.qcri.rheem.experiment.ExperimentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Entity {

    protected Object[] fields;
    private final static String dateRegex = "(\\d{4})-(\\d{2})-(\\d{2})";
    private final static Pattern pattern = Pattern.compile(dateRegex);


    public Entity(String data){
        this.fields = data.split("|");
    }

    public Entity(Object ... data){
        this.fields = data;
    }

    public String getString(int index){
        return (String)this.fields[index];
    }


    public long getLong(int index){
        try {
            return Long.parseLong((String)this.fields[index]);
        }catch (NumberFormatException e){
            throw new ExperimentException(e);
        }
    }

    public int getInt(int index){
        try {
            return Integer.parseInt((String)this.fields[index]);
        }catch (NumberFormatException e){
            throw new ExperimentException(e);
        }
    }

    public double getDouble(int index){
        try {
            return Double.parseDouble((String)this.fields[index]);
        }catch (NumberFormatException e){
            throw new ExperimentException(e);
        }
    }

    public int parseDate(int index){
        return Entity.dateValue(this.getString(index));
    }

    public static int dateValue(String date){
        Matcher matcher = pattern.matcher(date);
        String[] values = new String[3];
        int i = 0;
        while (matcher.find()){
            values[i] = matcher.group();
            i++;
        }
        return Integer.parseInt(values[0])*365
                + Integer.parseInt(values[1])*30
                + Integer.parseInt(values[2]);
    }

}
