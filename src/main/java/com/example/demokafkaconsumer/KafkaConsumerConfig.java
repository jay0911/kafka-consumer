package com.example.demokafkaconsumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.example.model.Employee;
import com.example.model.Student;

@Configuration
public class KafkaConsumerConfig {

	@Value("${kafka.boot.server}")
	private String kafkaServer;

	@Value("${kafka.consumer.group.id}")
	private String kafkaGroupId;
	
    @Bean
    public RetryPolicy retryPolicy() {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3);
        return simpleRetryPolicy;
    }

    @Bean
    public BackOffPolicy backOffPolicy() {
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(5000);
        return backOffPolicy;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy());
        retryTemplate.setBackOffPolicy(backOffPolicy());
        return retryTemplate;
    }


	@Bean
	public ConsumerFactory<String, Student> consumerConfig() {
	        Map<String, Object> props = new HashMap<>();
	        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
	        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
	        return new DefaultKafkaConsumerFactory<>(props, 
	                new StringDeserializer(), 
	                new JsonDeserializer<>(Student.class));
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Student>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Student> listener = new ConcurrentKafkaListenerContainerFactory<>();
		listener.setConsumerFactory(consumerConfig());
		listener.setRetryTemplate(retryTemplate());
		
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler((record, exception) -> {

        	System.out.println("failed to process kafka message (retries are exausted). topic name:" + record.topic() + " value:"
                    + record.value());   
        });
        
        listener.setErrorHandler(errorHandler);
		
		return listener;
	}
	
	
	@Bean
	public ConsumerFactory<String, Employee> employeeConfig() {
	        Map<String, Object> props = new HashMap<>();
	        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
	        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
	        return new DefaultKafkaConsumerFactory<>(props, 
	                new StringDeserializer(), 
	                new JsonDeserializer<>(Employee.class));
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Employee>> employeeKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Employee> listener = new ConcurrentKafkaListenerContainerFactory<>();
		listener.setConsumerFactory(employeeConfig());
		listener.setRetryTemplate(retryTemplate());
		
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler((record, exception) -> {

        	System.out.println("failed to process kafka message (retries are exausted). topic name:" + record.topic() + " value:"
                    + record.value());   
        });
        
        listener.setErrorHandler(errorHandler);
		
		return listener;
	}
}
