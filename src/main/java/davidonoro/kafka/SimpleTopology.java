package davidonoro.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Component
public class SimpleTopology {

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
     * Defines topology
     * @return
     */
    public Topology defineTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> inputStream = builder.stream(metricsRAWTopic);
        KStream<String,Integer> intStream = inputStream.flatMapValues(s->{
            try{
                return Collections.singletonList(Integer.valueOf(s));
            }catch (Exception e){
                System.out.println("Can't parse message: "+e.toString());
                return Collections.emptyList();
            }
        });

        KTable<Windowed<String>, Integer> groupedMetrics = intStream.groupBy((key, value)->key,
                Serialized.with(Serdes.String(),Serdes.Integer())).windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(windosSizeSeconds))).aggregate(
                ()-> 0,
                (String aggKey, Integer newValue, Integer aggValue)->{
                    Integer val = aggValue+newValue;
                    return val;
                },
                Materialized.<String,Integer, WindowStore<Bytes,byte[]>>as("GROUPING.WINDOW").withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer())
        );

        groupedMetrics.toStream().map((key,value)-> {
            System.out.println(key.key()+":"+value.toString());
            return KeyValue.pair(key.key(),value.toString());
        }).to(metricsOutputTopic);

        return builder.build();
    }
}
