package com.example.bff;

import com.example.bff.config.KafkaConsumerConfig;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class BffApplication {
    public static void main(String[] args) {
        SpringApplication.run(BffApplication.class, args);
    }
}

@Component
class KafkaOffsetResetOnStartup {
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetResetOnStartup.class);

    private final KafkaAdmin kafkaAdmin;
    private final KafkaConsumerConfig kafkaConsumerConfig;

    public KafkaOffsetResetOnStartup(KafkaAdmin kafkaAdmin, KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @PostConstruct
    public void resetOffsets() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            resetGroupToEarliest(admin, KafkaConsumerConfig.CAMPAIGN_GROUP_ID, "campaign-events");
            resetGroupToEarliest(admin, KafkaConsumerConfig.ADGROUP_GROUP_ID, "adgroup-events");
        } catch (Exception e) {
            log.warn("Startup offset reset skipped due to error: {}", e.getMessage());
        }
    }

    private void resetGroupToEarliest(AdminClient admin, String groupId, String topic) throws Exception {
        if (!topicExists(admin, topic)) {
            log.info("Topic {} not found; skipping offset reset for group {}", topic, groupId);
            return;
        }

        DescribeTopicsResult describeResult = admin.describeTopics(java.util.List.of(topic));
        TopicDescription description = describeResult.values().get(topic).get();
        java.util.List<TopicPartition> partitions = description.partitions().stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();

        java.util.Map<TopicPartition, OffsetSpec> request = new java.util.HashMap<>();
        partitions.forEach(tp -> request.put(tp, OffsetSpec.earliest()));

        ListOffsetsResult offsetsResult = admin.listOffsets(request);
        java.util.Map<TopicPartition, ListOffsetsResultInfo> offsets = offsetsResult.all().get();

        java.util.Map<TopicPartition, OffsetAndMetadata> target = offsets.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(java.util.Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset())));

        AlterConsumerGroupOffsetsResult alterResult = admin.alterConsumerGroupOffsets(groupId, target);
        alterResult.all().get();
        log.info("Reset offsets to earliest for group {} on topic {} ({} partitions)", groupId, topic, partitions.size());
    }

    private boolean topicExists(AdminClient admin, String topic) throws Exception {
        return admin.listTopics().listings().get().stream().map(TopicListing::name).anyMatch(topic::equals);
    }
}
