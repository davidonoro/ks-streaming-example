package davidonoro;

import davidonoro.kafka.MetricsNormalizer;
import davidonoro.model.RawMetric;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
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
import java.util.concurrent.TimeUnit;

public class TopologyWindowTests {

    TopologyTestDriver testDriver;
    String INPUT_TOPIC = "INPUT.TOPIC";
    String OUTPUT_TOPIC = "OUTPUT.TOPIC";

    @Before
    public void setup(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // EventProcessor is a <String,String> processor
        // so we set those serders
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(defineTopology(),config,0L);
    }

    /**
     * topology test
     */
    @Test
    public void testTopologyNoCorrelation() throws IOException {
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        testDriver.pipeInput(factory.create(INPUT_TOPIC,"k","2",1L));
        testDriver.pipeInput(factory.create(INPUT_TOPIC,"k","2",1L));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

        Assert.assertNull(outputRecord);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    /**
     * Defines topology
     * @return
     */
    public Topology defineTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> inputStream = builder.stream(INPUT_TOPIC);
        KStream<String,Integer> intStream = inputStream.mapValues(s->Integer.valueOf(s));

        KTable<Windowed<String>, Integer> groupedMetrics = intStream.groupBy((key, value)->key,
                Serialized.with(Serdes.String(),Serdes.Integer())).windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10))).aggregate(
                ()-> 0,
                (String aggKey, Integer newValue, Integer aggValue)->{
                    Integer val = aggValue+newValue;
                    return val;
                },
                Materialized.<String,Integer, WindowStore<Bytes,byte[]>>as("GROUPING.WINDOW").withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer())
        );

        groupedMetrics.toStream().map((key,value)-> KeyValue.pair(key.key(),value.toString())).to(OUTPUT_TOPIC);

        return builder.build();
    }
}

