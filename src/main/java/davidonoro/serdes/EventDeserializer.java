package davidonoro.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class EventDeserializer<T> implements Deserializer<T> {

    private final static ObjectMapper mapper = new ObjectMapper();
    private Class<T> clazz;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        clazz = (Class<T>)map.get("serializedClass");
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if(bytes == null){
            return null;
        }else{
            try {
                return mapper.readValue(bytes,clazz);
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }

    }

    @Override
    public void close() {

    }
}
