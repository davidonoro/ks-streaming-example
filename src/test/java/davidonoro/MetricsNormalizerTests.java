package davidonoro;

import davidonoro.kafka.MetricsNormalizer;
import davidonoro.kafka.KafkaConfigRetriever;
import davidonoro.model.NormalizedMetric;
import davidonoro.model.RawMetric;
import davidonoro.serdes.EventHelper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class})
@TestPropertySource(locations="classpath:application.properties")
public class MetricsNormalizerTests {

    /**
     * Topology builder with pur custom logic
     */
    @Autowired
    private MetricsNormalizer topologyBuilder;

    TopologyTestDriver testDriver;

    @Before
    public void setup(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // EventProcessor is a <String,String> processor
        // so we set those serders
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(topologyBuilder.defineTopology(),config,0L);
    }

    /**
     * topology test
     */
    @Test
    public void testTopologyNoCorrelation() throws IOException {

        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

        // Events injection
        String msgCPU = "{\"idContainer\":\"id2\",\"timestamp\":10000,\"metrics\":{\"container_cpu_limit\":1,\"container_cpu_usage\":1}}";
        String msgMem="{\"idContainer\":\"id2\",\"timestamp\":10000,\"metrics\":{\"container_memory_limit\":1,\"container_memory_usage\":1,\"container_memory_usage_percent\":1}}";
        testDriver.pipeInput(recordFactory.create(topologyBuilder.metricsRAWTopic,"host",msgCPU,0L));
        testDriver.pipeInput(recordFactory.create(topologyBuilder.metricsRAWTopic,"host",msgMem,1L));


        //Advance time to not  execute window
        //testDriver.advanceWallClockTime(21000L)

        // Test output record
        Assert.assertNotNull(testDriver.readOutput(topologyBuilder.metricsOutputTopic, new StringDeserializer(), new StringDeserializer()));
    }

    @After
    public void tearDown() {
        testDriver.close();
    }
}

