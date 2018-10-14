package davidonoro.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
public class CustomStreamBuilder {

    /**
     * Kafka config retriever
     */
    @Autowired
    private KafkaConfigRetriever configRetriever;

    /**
     * Topology builder with pur custom logic
     */
    @Autowired
    private SimpleTopology topologyBuilder;

    /**
     * Object which performs the continuous computation
     */
    private KafkaStreams stream;

    /**
     * Class Logger
     */
    private final Logger logger = LoggerFactory.getLogger(CustomStreamBuilder.class);



    /**
     * method that initalises the computation according to a builder and a configuration
     * The annotation means that the bean is fully initialised and the dependices can be used,
     * so we can start our streaming
     */
    @PostConstruct
    public void runStream(){


        stream = new KafkaStreams(topologyBuilder.defineTopology(),configRetriever.getConfig());
        stream.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                logger.error("Uncaught exception: "+throwable.getMessage());
                closeStream();
                Thread.currentThread().interrupt();
                Runtime.getRuntime().exit(1);
            }
        });

        stream.start();

        //Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeStream));
    }




    /**
     * It stops the streaming computation
     */
    @PreDestroy
    public void closeStream(){
        stream.close(1, TimeUnit.SECONDS);
    }

}
