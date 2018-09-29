package davidonoro.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SerdeFactory {

    public static <T> Serde<T> createSerde(Class<T> clazz){
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("serializedClass", clazz);

        Serializer<T> ser = new EventSerializer<T>();
        ser.configure(serdeProps,false);

        Deserializer<T> des = new EventDeserializer<T>();
        des.configure(serdeProps,false);

        return Serdes.serdeFrom(ser,des);
    }
}
