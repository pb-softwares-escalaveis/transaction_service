package br.com.infnet.transactionService.integration;

import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class FlowTestOutboundCapture {

    private FlowTestOutboundCapture() {
    }

    public static TransactionStatusEvent awaitStatusEvent(
            EmbeddedKafkaBroker broker,
            String topic,
            UUID correlationId,
            TransactionStatus expectedStatus,
            Duration timeout) {

        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "flow-outbound-" + UUID.randomUUID(),
                "earliest",
                broker);

        JacksonJsonDeserializer<TransactionStatusEvent> deserializer =
                new JacksonJsonDeserializer<>(TransactionStatusEvent.class);
        deserializer.addTrustedPackages("br.com.infnet.transactionService.events");
        deserializer.setUseTypeHeaders(false);

        try (Consumer<String, TransactionStatusEvent> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), deserializer)) {

            consumer.subscribe(List.of(topic));
            broker.consumeFromAnEmbeddedTopic(consumer, topic);

            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500)).records(topic);
                for (ConsumerRecord<String, TransactionStatusEvent> record : records) {
                    TransactionStatusEvent event = record.value();
                    if (event != null
                            && correlationId.equals(event.correlationId())
                            && expectedStatus == event.status()) {
                        return event;
                    }
                }
            }
        }
        return null;
    }
}
