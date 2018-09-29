package davidonoro.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaConfigRetriever {

    /**
     * ValidatorApp id
     */
    @Value("${application.id}")
    private String appId;

    /**
     * Kafka broker
     */
    @Value("${bootstrap.servers}")
    private String kafkaBroker;

    /**
     * Max number of threads
     */
    @Value("${num.stream.threads}")
    private int maxNumThreads;

    /**
     * Returns the configuration of the streaming: kafka server, aplication id
     * @return
     */
    public StreamsConfig getConfig(){
        return new StreamsConfig(getProps());
    }

    /**
     * Returns the configuration of the streaming: kafka server, aplication id
     * @return
     */
    public Properties getProps(){
        Properties props = new Properties();

        // App
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,maxNumThreads);

        // Kafka
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBroker);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        return props;
    }
}
