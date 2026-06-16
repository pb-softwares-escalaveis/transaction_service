package br.com.infnet.transactionService.config;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;
import br.com.infnet.transactionService.events.outbound.PaymentRequestedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionClosedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@Configuration
@EnableKafka
public class KafkaConfig {

    private static final String TRUSTED_PACKAGES = "br.com.infnet.transactionService.events";

    @Bean
    public ConsumerFactory<String, AuctionEndedWithWinnerEvent> auctionEndedWithWinnerConsumerFactory(
            KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, AuctionEndedWithWinnerEvent.class);
    }

    @Bean
    public ConsumerFactory<String, PaymentCreatedEvent> paymentCreatedConsumerFactory(
            KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, PaymentCreatedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, PaymentCreatedFailedEvent> paymentCreatedFailedConsumerFactory(
            KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, PaymentCreatedFailedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, PaymentReceivedEvent> paymentReceivedConsumerFactory(
            KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, PaymentReceivedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, PaymentExpiredEvent> paymentExpiredConsumerFactory(
            KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, PaymentExpiredEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AuctionEndedWithWinnerEvent>
            auctionEndedWithWinnerKafkaListenerContainerFactory(
                    ConsumerFactory<String, AuctionEndedWithWinnerEvent> consumerFactory) {
        return createListenerContainerFactory(consumerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCreatedEvent>
            paymentCreatedKafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentCreatedEvent> consumerFactory) {
        return createListenerContainerFactory(consumerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCreatedFailedEvent>
            paymentCreatedFailedKafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentCreatedFailedEvent> consumerFactory) {
        return createListenerContainerFactory(consumerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentReceivedEvent>
            paymentReceivedKafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentReceivedEvent> consumerFactory) {
        return createListenerContainerFactory(consumerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentExpiredEvent>
            paymentExpiredKafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentExpiredEvent> consumerFactory) {
        return createListenerContainerFactory(consumerFactory);
    }

    @Bean
    public ProducerFactory<String, PaymentRequestedEvent> paymentRequestedProducerFactory(
            KafkaProperties kafkaProperties) {
        return createProducerFactory(kafkaProperties, PaymentRequestedEvent.class);
    }

    @Bean
    public KafkaTemplate<String, PaymentRequestedEvent> paymentRequestedKafkaTemplate(
            ProducerFactory<String, PaymentRequestedEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, TransactionStatusEvent> transactionStatusProducerFactory(
            KafkaProperties kafkaProperties) {
        return createProducerFactory(kafkaProperties, TransactionStatusEvent.class);
    }

    @Bean
    public KafkaTemplate<String, TransactionStatusEvent> transactionStatusKafkaTemplate(
            ProducerFactory<String, TransactionStatusEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, TransactionClosedEvent> transactionClosedProducerFactory(
            KafkaProperties kafkaProperties) {
        return createProducerFactory(kafkaProperties, TransactionClosedEvent.class);
    }

    @Bean
    public KafkaTemplate<String, TransactionClosedEvent> transactionClosedKafkaTemplate(
            ProducerFactory<String, TransactionClosedEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(
            KafkaProperties kafkaProperties,
            Class<T> eventType) {

        JacksonJsonDeserializer<T> deserializer = new JacksonJsonDeserializer<>(eventType);
        deserializer.addTrustedPackages(TRUSTED_PACKAGES);
        deserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    private <T> ProducerFactory<String, T> createProducerFactory(
            KafkaProperties kafkaProperties,
            Class<T> eventType) {

        JacksonJsonSerializer<T> serializer = new JacksonJsonSerializer<T>().copyWithType(eventType);
        serializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                serializer);
    }
}
