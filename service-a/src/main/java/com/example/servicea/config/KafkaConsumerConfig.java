package com.example.servicea.config;

import com.example.servicea.model.CampaignEvent;
import com.example.servicea.model.CreateAdGroupCommand;
import com.example.servicea.model.CreateCampaignCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
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

    private Map<String, Object> baseProps(Class<?> valueType, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, valueType);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> campaignConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseProps(CreateCampaignCommand.class, "service-a-campaign-group"));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> campaignListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(campaignConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> campaignEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseProps(CampaignEvent.class, "service-a-campaign-event-group"));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> campaignEventListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(campaignEventConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> adGroupConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseProps(CreateAdGroupCommand.class, "service-a-adgroup-group"));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> adGroupListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(adGroupConsumerFactory());
        return factory;
    }
}
