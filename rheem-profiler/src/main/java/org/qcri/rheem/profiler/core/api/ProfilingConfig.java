package org.qcri.rheem.profiler.core.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by migiwara on 07/06/17.
 */
public class ProfilingConfig {



    private String profilingPlanGenerationEnumeration;
    private String profilingConfigurationEnumeration;



    private String profilingPlateform;

    public boolean isBushyGeneration() {
        return bushyGeneration;
    }

    public void setBushyGeneration(boolean bushyGeneration) {
        this.bushyGeneration = bushyGeneration;
    }

    private boolean bushyGeneration;

    private List<Long> inputCardinality = new ArrayList<>();
    private List<Integer> dataQuantaSize = new ArrayList<>();
    private List<Integer> UdfsComplexity = new ArrayList<>();
    private List<Integer> inputRatio = new ArrayList<>();


    public void setInputCardinality(List<Long> inputCardinality) {
        this.inputCardinality = inputCardinality;
    }

    public void setDataQuantaSize(List<Integer> dataQuantaSize) {
        this.dataQuantaSize = dataQuantaSize;
    }

    public void setUdfsComplexity(List<Integer> udfsComplexity) {
        UdfsComplexity = udfsComplexity;
    }

    public void setInputRatio(List<Integer> inputRatio) {
        this.inputRatio = inputRatio;
    }


    public String getProfilingPlanGenerationEnumeration() {
        return profilingPlanGenerationEnumeration;
    }

    public void setProfilingPlanGenerationEnumeration(String profilingPlanGenerationEnumeration) {
        this.profilingPlanGenerationEnumeration = profilingPlanGenerationEnumeration;
    }

    public String getProfilingConfigurationEnumeration() {
        return profilingConfigurationEnumeration;
    }

    public void setProfilingConfigurationEnumeration(String profilingConfigurationEnumeration) {
        this.profilingConfigurationEnumeration = profilingConfigurationEnumeration;
    }

    public String getProfilingPlateform() {
        return profilingPlateform;
    }

    public void setProfilingPlateform(String profilingPlateform) {
        this.profilingPlateform = profilingPlateform;
    }

    public List<Long> getInputCardinality() {
        return inputCardinality;
    }

    public List<Integer> getDataQuantaSize() {
        return dataQuantaSize;
    }

    public List<Integer> getUdfsComplexity() {
        return UdfsComplexity;
    }

    public List<Integer> getInputRatio() {
        return inputRatio;
    }
}
