package br.com.infnet.transactionService.integration;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
public class FlowTestKafkaProducers {

    @Bean
    KafkaTemplate<String, AuctionEndedWithWinnerEvent> auctionEventKafkaTemplate(
            EmbeddedKafkaBroker embeddedKafka) {
        return createTemplate(embeddedKafka, AuctionEndedWithWinnerEvent.class);
    }

    @Bean
    KafkaTemplate<String, PaymentCreatedEvent> paymentCreatedKafkaTemplate(
            EmbeddedKafkaBroker embeddedKafka) {
        return createTemplate(embeddedKafka, PaymentCreatedEvent.class);
    }

    @Bean
    KafkaTemplate<String, PaymentCreatedFailedEvent> paymentCreatedFailedKafkaTemplate(
            EmbeddedKafkaBroker embeddedKafka) {
        return createTemplate(embeddedKafka, PaymentCreatedFailedEvent.class);
    }

    @Bean
    KafkaTemplate<String, PaymentReceivedEvent> paymentReceivedKafkaTemplate(
            EmbeddedKafkaBroker embeddedKafka) {
        return createTemplate(embeddedKafka, PaymentReceivedEvent.class);
    }

    @Bean
    KafkaTemplate<String, PaymentExpiredEvent> paymentExpiredKafkaTemplate(
            EmbeddedKafkaBroker embeddedKafka) {
        return createTemplate(embeddedKafka, PaymentExpiredEvent.class);
    }

    private static <T> KafkaTemplate<String, T> createTemplate(
            EmbeddedKafkaBroker embeddedKafka,
            Class<T> eventType) {

        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        JacksonJsonSerializer<T> serializer = new JacksonJsonSerializer<T>().copyWithType(eventType);
        serializer.setAddTypeInfo(false);

        ProducerFactory<String, T> factory =
                new DefaultKafkaProducerFactory<>(props, new StringSerializer(), serializer);
        return new KafkaTemplate<>(factory);
    }
}
