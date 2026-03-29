package com.example.campaignmanager.config;

import com.example.campaignmanager.model.Envelope;
import com.example.campaignmanager.service.StateRebuildRebalanceListener;
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

    private final StateRebuildRebalanceListener stateRebuildRebalanceListener;

    public KafkaConsumerConfig(StateRebuildRebalanceListener stateRebuildRebalanceListener) {
        this.stateRebuildRebalanceListener = stateRebuildRebalanceListener;
    }

    /* ------------------------------------------------------------------ */
    /*  Shared consumer props                                              */
    /* ------------------------------------------------------------------ */

    private Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Envelope.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /* ------------------------------------------------------------------ */
    /*  State-rebuild factory – seeks to 0, records end offsets             */
    /* ------------------------------------------------------------------ */

    @Bean
    public ConsumerFactory<String, Envelope> stateRebuildConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseProps());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Envelope> stateRebuildListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Envelope> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stateRebuildConsumerFactory());
        factory.getContainerProperties().setConsumerRebalanceListener(stateRebuildRebalanceListener);
        // Enable idle event so CampaignEventListener can detect the empty-topic edge case
        factory.getContainerProperties().setIdleEventInterval(5000L);
        return factory;
    }

    /* ------------------------------------------------------------------ */
    /*  Command factory – normal offsets, auto-start disabled               */
    /* ------------------------------------------------------------------ */

    @Bean
    public ConsumerFactory<String, Envelope> commandConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseProps());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Envelope> commandListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Envelope> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commandConsumerFactory());
        factory.setAutoStartup(false);
        return factory;
    }

    /* ------------------------------------------------------------------ */
    /*  Default factory (for any listener without explicit factory ref)     */
    /* ------------------------------------------------------------------ */

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Envelope> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Envelope> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stateRebuildConsumerFactory());
        return factory;
    }
}
