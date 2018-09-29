package davidonoro.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import davidonoro.model.NormalizedMetric;
import davidonoro.model.RawMetric;
import davidonoro.serdes.EventHelper;
import davidonoro.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Component
public class MetricsNormalizer {

    /**
     * Metrics input topic
     */
    @Value("${metrics.raw.topic}")
    public String metricsRAWTopic;

    /**
     * Normalized metrics output topic
     */
    @Value("${metrics.eventbus.topic}")
    public String metricsOutputTopic;

    /**
     * Normalized metrics output topic
     */
    @Value("${aggregation.window.size.secs}")
    public Integer windosSizeSeconds;

     /**
      * Class Logger
      */
    private final Logger logger = LoggerFactory.getLogger(MetricsNormalizer.class);


    /**
     * Defines the streaming logic
     * @return The topology with the logic
     */
    public Topology defineTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        // Creation of the metrics store
        Serde<RawMetric> rawMetricSerde = SerdeFactory.createSerde(RawMetric.class);
        StoreBuilder<KeyValueStore<String,RawMetric>> metricsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("METRICS_STORE"),
                Serdes.String(),
                rawMetricSerde
        );
        builder.addStateStore(metricsStoreBuilder);

        // create yhe stream
        KStream<String,String> inputStream = builder.stream(metricsRAWTopic);

        // Parse input events
        KStream<String,RawMetric> metricStream = parseRawMetrics(inputStream);

        // Group metrics in a window
        KStream<String,RawMetric> groupedMetrics = groupMetrics(metricStream);

        // Obtain normalized metrics
        KStream<String,NormalizedMetric> normalizedMetrics = groupedMetrics.transform(MetricsTransformer::new,"METRICS_STORE");

        // Write into output topic
        send2OutputTopic(normalizedMetrics);

        // Creation of the topology
        return builder.build();
    }

    /**
     * Writes metrics
     */
    public void send2OutputTopic(KStream<String,NormalizedMetric> stream){
        EventHelper<NormalizedMetric> helper = new EventHelper<NormalizedMetric>(NormalizedMetric.class);

        stream.flatMapValues(metric->{
            try {
                return Collections.singletonList(helper.toString(metric));
            } catch (JsonProcessingException e) {
                logger.error("Error writing event: "+e.toString());
                return Collections.emptyList();
            }
        }).to(metricsOutputTopic);
    }

    /**
     * Groups metrics received for a specific container id within a certain window time
     * @param metricStream
     * @return
     */
    private KStream<String,RawMetric> groupMetrics(KStream<String,RawMetric> metricStream) {
        Serde<RawMetric> rawMetricSerde = SerdeFactory.createSerde(RawMetric.class);

        KTable<Windowed<String>, RawMetric> groupedMetrics = metricStream.groupBy((key,metric)->metric.getIdContainer()+"-"+metric.getTimestamp(),
                Serialized.with(Serdes.String(),rawMetricSerde)).windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(windosSizeSeconds))).aggregate(
                ()-> new RawMetric(),
                (String aggKey, RawMetric newMetric, RawMetric aggMetric)->{
                    aggMetric.merge(newMetric);
                    return aggMetric;
                },
                Materialized.<String,RawMetric,WindowStore<Bytes,byte[]>>as("GROUPING.WINDOW").withKeySerde(Serdes.String()).withValueSerde(rawMetricSerde)
        );


        return groupedMetrics.toStream().map((key,metric)->KeyValue.pair(metric.getIdContainer(),metric));
    }

    /**
     * Parsing events in raw metrics. Use of flatMap pattern to extract invalid messages from the stream
     * @param inputStream
     * @return
     */
    private KStream<String, RawMetric> parseRawMetrics(KStream<String, String> inputStream) {
        EventHelper<RawMetric> rawMetricEventHelper = new EventHelper<RawMetric>(RawMetric.class);

        return inputStream.flatMapValues(str->{
            try {
                return Collections.singletonList(rawMetricEventHelper.extract(str));
            } catch (IOException e) {
                logger.error("Error parsing event: "+e.getMessage());
                return Collections.emptyList();
            }
        });
    }
}
