package davidonoro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableAutoConfiguration
@SpringBootApplication
public class KSStreamingExample {

    public static void main(String args[]){
        SpringApplication.run(KSStreamingExample.class,args);
    }
}
