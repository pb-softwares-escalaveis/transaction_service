package br.com.infnet.transactionService.kafka;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.repository.TransactionHistoryRepository;
import br.com.infnet.transactionService.repository.TransactionRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_WAITING_FOR_PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "auctions.lot.ended-with-winner",
                "transactions.payment.requested",
                "transactions.status.created",
                "transactions.status.waiting-for-payment"
        })
@Import(AuctionEndedWithWinnerConsumerTest.AuctionEventProducerConfig.class)
class AuctionEndedWithWinnerConsumerTest {

    private static final String TOPIC = "auctions.lot.ended-with-winner";

    @Autowired
    private KafkaTemplate<String, AuctionEndedWithWinnerEvent> auctionEventKafkaTemplate;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionHistoryRepository transactionHistoryRepository;

    private UUID correlationId;
    private AuctionEndedWithWinnerEvent event;

    @BeforeEach
    void setUp() {
        transactionHistoryRepository.deleteAll();
        transactionRepository.deleteAll();
        correlationId = UUID.randomUUID();
        event = new AuctionEndedWithWinnerEvent(
                correlationId,
                100L,
                UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-111111111111"),
                UUID.fromString("bbbbbbbb-cccc-dddd-eeee-222222222222"),
                new BigDecimal("250.50"),
                Instant.parse("2026-06-14T10:00:00Z"),
                "Leilão Teste",
                "https://example.com/thumb.jpg");
    }

    @Test
    void shouldCreateTransactionWhenAuctionEndedWithWinnerIsReceived() {
        auctionEventKafkaTemplate.send(TOPIC, correlationId.toString(), event);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(transactionRepository.count()).isEqualTo(1);
            assertThat(transactionRepository.findByCorrelationId(correlationId))
                    .isPresent()
                    .get()
                    .satisfies(transaction -> {
                        assertThat(transaction.getStatus()).isEqualTo(TRANSACTION_WAITING_FOR_PAYMENT);
                        assertThat(transaction.getAuctionId()).isEqualTo(100L);
                        assertThat(transaction.getAmountInCents()).isEqualTo(25050);
                    });
        });
    }

    @Test
    void shouldIgnoreDuplicateEventWithSameCorrelationId() {
        auctionEventKafkaTemplate.send(TOPIC, correlationId.toString(), event);
        await().atMost(Duration.ofSeconds(15)).until(() -> transactionRepository.count() == 1);

        auctionEventKafkaTemplate.send(TOPIC, correlationId.toString(), event);

        await().pollDelay(Duration.ofMillis(500))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(transactionRepository.count()).isEqualTo(1));
    }

    @TestConfiguration
    static class AuctionEventProducerConfig {

        @Bean
        KafkaTemplate<String, AuctionEndedWithWinnerEvent> auctionEventKafkaTemplate(
                EmbeddedKafkaBroker embeddedKafka) {

            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            JacksonJsonSerializer<AuctionEndedWithWinnerEvent> serializer =
                    new JacksonJsonSerializer<AuctionEndedWithWinnerEvent>()
                            .copyWithType(AuctionEndedWithWinnerEvent.class);
            serializer.setAddTypeInfo(false);

            ProducerFactory<String, AuctionEndedWithWinnerEvent> factory =
                    new DefaultKafkaProducerFactory<>(props, new StringSerializer(), serializer);
            return new KafkaTemplate<>(factory);
        }
    }
}
