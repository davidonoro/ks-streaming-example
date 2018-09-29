package davidonoro.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

public class RawMetric {

    @JsonIgnore
    public static final String MEMORY_LIMIT = "container_memory_limit";

    @JsonIgnore
    public static final String MEMORY_USAGE = "container_memory_usage";

    @JsonIgnore
    public static final String MEMORY_USAGE_PERCENT = "container_memory_usage_percent";

    @JsonIgnore
    public static final String CPU_LIMIT = "container_cpu_limit";

    @JsonIgnore
    public static final String CPU_USAGE = "container_cpu_usage";

    @JsonProperty("idContainer")
    private String idContainer;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("metrics")
    private HashMap<String,Float> metrics;

    /**
     * Void constructor
     */
    public RawMetric(){
        metrics = new HashMap<String,Float>();
    }

    /**
     * Merge metrics
     */
    public void merge(RawMetric newMetric){
        this.idContainer = newMetric.getIdContainer();
        this.timestamp = newMetric.getTimestamp();

        for(String literal : newMetric.metrics.keySet()){
            if(! metrics.containsKey(literal)){
                metrics.put(literal,newMetric.metrics.get(literal));
            }
        }
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

    public HashMap<String, Float> getMetrics() {
        return metrics;
    }

    public void setMetrics(HashMap<String, Float> metrics) {
        this.metrics = metrics;
    }
}
