package com.rahul.kafkatimetravelengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaTimeTravelEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTimeTravelEngineApplication.class, args);
    }
}
