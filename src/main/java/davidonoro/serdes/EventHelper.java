package davidonoro.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EventHelper<T> {

    /**
     * Mapper to read json msgs
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(EventHelper.class);


    /**
     * Class
     */
    private final Class<T> clazz;


    public EventHelper(Class<T> clazz){
        this.clazz = clazz;
    }


    /**
     * Extracts a Type object from a text
     * @param txt Text with the DockerEvent
     * @return It returns an empty object with null fields if the text doesn't match the expected body
     * @exception IOException
     */
    public T extract(String txt) throws IOException { ;
        return mapper.readValue(txt,clazz);
    }

    /**
     * To String
     * @param evt
     * @return
     */
    public String toString(T evt) throws JsonProcessingException {
        /*try {
            return mapper.writeValueAsString(evt);
        } catch (JsonProcessingException e) {
            return "";
        }*/
        return mapper.writeValueAsString(evt);
    }
}
