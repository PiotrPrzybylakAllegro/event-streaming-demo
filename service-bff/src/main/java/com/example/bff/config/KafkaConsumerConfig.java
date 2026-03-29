package com.example.bff.config;

import com.example.bff.model.AdGroupEvent;
import com.example.bff.model.CampaignEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public static final String CAMPAIGN_GROUP_ID = "service-bff-campaign-events";
    public static final String ADGROUP_GROUP_ID = "service-bff-adgroup-events";

    @Bean
    public Map<String, Object> campaignConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CampaignEvent.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.bff.model,com.example.servicea.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CAMPAIGN_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(campaignConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setConsumerRebalanceListener(new SeekToBeginningRebalanceListener());
        return factory;
    }

    @Bean
    public Map<String, Object> adGroupConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AdGroupEvent.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.bff.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ADGROUP_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> adGroupConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(adGroupConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> adGroupListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(adGroupConsumerFactory());
        factory.getContainerProperties().setConsumerRebalanceListener(new SeekToBeginningRebalanceListener());
        return factory;
    }

    private static class SeekToBeginningRebalanceListener implements org.springframework.kafka.listener.ConsumerAwareRebalanceListener {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, java.util.Collection<TopicPartition> partitions) {
            consumer.seekToBeginning(partitions);
        }
    }
}
