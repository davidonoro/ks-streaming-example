package davidonoro.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import davidonoro.model.NormalizedMetric;
import davidonoro.model.RawMetric;
import davidonoro.serdes.EventHelper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsTransformer implements Transformer<String,RawMetric,KeyValue<String,NormalizedMetric>> {

    /**
     * Context
     */
    private ProcessorContext context;

    /**
     * Store container_id/SingleMetric
     */
    private KeyValueStore<String,RawMetric> store;

    /**
     * Log4j
     */
    Logger logger = LoggerFactory.getLogger(MetricsTransformer.class);

    EventHelper<RawMetric> helper = new EventHelper<RawMetric>(RawMetric.class);

    /**
     * Initilizes transformer
     * @param processorContext
     */
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.store = (KeyValueStore<String,RawMetric>) this.context.getStateStore("METRICS_STORE");

        // we can delete all metrics from the store. Worst case, we will lose 1 metric
        // when the store is empty
        this.context.schedule(60000,PunctuationType.WALL_CLOCK_TIME, (timestamp)->{
            eraseCacheInfo();
        });
    }

    /**
     *
     * @param key
     * @param rawMetric
     * @return
     */
    @Override
    public KeyValue<String, NormalizedMetric> transform(String key, RawMetric rawMetric) {
       if(store.get(rawMetric.getIdContainer()) != null){
           try {
               logger.info("Calculating normalized metric "+helper.toString(rawMetric));
           } catch (JsonProcessingException e) {
               e.printStackTrace();
               return null;
           }
           // Calculate Normalized metric
           NormalizedMetric metric = new NormalizedMetric();
           metric.setIdContainer(rawMetric.getIdContainer());
           metric.setTimestamp(rawMetric.getTimestamp());
           metric.setCpuLimit(rawMetric.getMetrics().get(RawMetric.CPU_LIMIT));
           metric.setCpuUsage(rawMetric.getMetrics().get(RawMetric.CPU_USAGE));
           metric.setMemoryLimit(rawMetric.getMetrics().get(RawMetric.MEMORY_LIMIT));
           metric.setMemoryUsage(rawMetric.getMetrics().get(RawMetric.MEMORY_USAGE));
           metric.setMemoryUsagePercent(rawMetric.getMetrics().get(RawMetric.MEMORY_USAGE_PERCENT));
           metric.setCpuUsagePercent(NormalizedMetric.calculateCPUUsagePercent(store.get(rawMetric.getIdContainer()),rawMetric));

           // stores the actual metric
           store.put(rawMetric.getIdContainer(),rawMetric);

           return KeyValue.pair(key,metric);

       }else{
           // No previous metric. Store actual metric but nothing to
           try {
               logger.info("No previous metric. Storing metric "+helper.toString(rawMetric));
           } catch (JsonProcessingException e) {
               e.printStackTrace();
               return null;
           }
           store.put(rawMetric.getIdContainer(),rawMetric);
           return null;
       }
    }

    @Override
    public KeyValue<String, NormalizedMetric> punctuate(long l) {
        return null;
    }

    /**
     * We can implemment here the logic to clean the store
     */
    public void eraseCacheInfo(){
        logger.info("Deleting metrics data in cache...");
        KeyValueIterator<String,RawMetric> iterator = store.all();
        while (iterator.hasNext()){
            store.delete(iterator.next().key);
        }
    }

    @Override
    public void close() {
        //nothing to do here
    }
}
