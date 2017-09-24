package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

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
    private List<Long> inputCardinality = new ArrayList<>();
    private List<Integer> dataQuantaSize = new ArrayList<>();
    private List<Integer> UdfsComplexity = new ArrayList<>();
    private List<Integer> inputRatio = new ArrayList<>();
    private List<String> profilingPlateforms;
    private List<DataSetType> dataTypes;

    public List<String> getUnaryExecutionOperators() {
        return unaryExecutionOperators;
    }

    public void setUnaryExecutionOperators(List<String> unaryExecutionOperators) {
        this.unaryExecutionOperators = unaryExecutionOperators;
    }

    public List<String> getBinaryExecutionOperators() {
        return binaryExecutionOperators;
    }

    public void setBinaryExecutionOperators(List<String> binaryExecutionOperators) {
        this.binaryExecutionOperators = binaryExecutionOperators;
    }

    public List<String> getSinkExecutionOperators() {
        return sinkExecutionOperators;
    }

    public void setSinkExecutionOperators(List<String> sinkExecutionOperators) {
        this.sinkExecutionOperators = sinkExecutionOperators;
    }

    public List<String> getSourceExecutionOperators() {
        return sourceExecutionOperators;
    }

    public void setSourceExecutionOperators(List<String> sourceExecutionOperators) {
        this.sourceExecutionOperators = sourceExecutionOperators;
    }

    private List<String> unaryExecutionOperators = new ArrayList<>();
    private List<String> binaryExecutionOperators = new ArrayList<>();

    public List<String> getLoopExecutionOperators() {
        return loopExecutionOperators;
    }

    public void setLoopExecutionOperators(List<String> loopExecutionOperators) {
        this.loopExecutionOperators = loopExecutionOperators;
    }

    private List<String> loopExecutionOperators = new ArrayList<>();

    private List<String> sinkExecutionOperators = new ArrayList<>();
    private List<String> sourceExecutionOperators = new ArrayList<>();




    public boolean isBushyGeneration() {
        return bushyGeneration;
    }

    public void setBushyGeneration(boolean bushyGeneration) {
        this.bushyGeneration = bushyGeneration;
    }

    private boolean bushyGeneration;

    public List<DataSetType> getDataType() {
        return dataTypes;
    }

    public void setDataType(List<DataSetType> dataType) {
        this.dataTypes = dataType;
    }



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

    public List<String> getProfilingPlateform() {
        return profilingPlateforms;
    }

    public void setProfilingPlateform(List<String> profilingPlateform) {
        this.profilingPlateforms = profilingPlateform;
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
