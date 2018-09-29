package davidonoro.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NormalizedMetric {

    @JsonProperty("idContainer")
    private String idContainer;

    @JsonProperty("timestamp")
    private Long timestamp;

    private Float memoryLimit;
    private Float memoryUsage;
    private Float memoryUsagePercent;
    private Float cpuLimit;
    private Float cpuUsage;
    private Float cpuUsagePercent;

    /**
     * Calculates the cpu usage percent as (cpuUsageT2-cpuUsageT1)
     */
    public static float calculateCPUUsagePercent(RawMetric oldM, RawMetric newM){
        return 100*(newM.getMetrics().get(RawMetric.CPU_USAGE) - oldM.getMetrics().get(RawMetric.CPU_USAGE)/(newM.getTimestamp()-oldM.getTimestamp()));
    }

    // Getters n setters
    public String getIdContainer() {
        return idContainer;
    }

    public void setIdContainer(String idContainer) {
        this.idContainer = idContainer;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Float getMemoryLimit() {
        return memoryLimit;
    }

    public void setMemoryLimit(Float memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    public Float getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Float memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public Float getMemoryUsagePercent() {
        return memoryUsagePercent;
    }

    public void setMemoryUsagePercent(Float memoryUsagePercent) {
        this.memoryUsagePercent = memoryUsagePercent;
    }

    public Float getCpuLimit() {
        return cpuLimit;
    }

    public void setCpuLimit(Float cpuLimit) {
        this.cpuLimit = cpuLimit;
    }

    public Float getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(Float cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public Float getCpuUsagePercent() {
        return cpuUsagePercent;
    }

    public void setCpuUsagePercent(Float cpuUsagePercent) {
        this.cpuUsagePercent = cpuUsagePercent;
    }
}
