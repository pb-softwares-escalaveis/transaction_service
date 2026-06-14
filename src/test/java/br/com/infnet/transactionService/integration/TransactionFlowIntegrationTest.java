package br.com.infnet.transactionService.integration;

import br.com.infnet.transactionService.controller.TransactionController;
import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import br.com.infnet.transactionService.repository.TransactionHistoryRepository;
import br.com.infnet.transactionService.repository.TransactionRepository;
import br.com.infnet.transactionService.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static br.com.infnet.transactionService.enums.TransactionStatus.DELIVERY_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_PAYMENT_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_WAITING_FOR_PAYMENT;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_AUCTION;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_PAYMENT_CREATED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_PAYMENT_EXPIRED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_PAYMENT_RECEIVED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_DELIVERY_PENDING;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_FINISHED;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_PAYMENT_PENDING;
import static br.com.infnet.transactionService.integration.FlowTestFixtures.TOPIC_STATUS_WAITING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
                FlowTestFixtures.TOPIC_AUCTION,
                FlowTestFixtures.TOPIC_PAYMENT_CREATED,
                FlowTestFixtures.TOPIC_PAYMENT_CREATED_FAILED,
                FlowTestFixtures.TOPIC_PAYMENT_RECEIVED,
                FlowTestFixtures.TOPIC_PAYMENT_EXPIRED,
                FlowTestFixtures.TOPIC_PAYMENT_REQUESTED,
                FlowTestFixtures.TOPIC_STATUS_CREATED,
                FlowTestFixtures.TOPIC_STATUS_WAITING,
                FlowTestFixtures.TOPIC_STATUS_PAYMENT_PENDING,
                FlowTestFixtures.TOPIC_STATUS_CLOSED_PAYMENT_CREATED_FAILED,
                FlowTestFixtures.TOPIC_STATUS_DELIVERY_PENDING,
                FlowTestFixtures.TOPIC_STATUS_FINISHED,
                FlowTestFixtures.TOPIC_STATUS_CLOSED_PAYMENT_FAILED,
                FlowTestFixtures.TOPIC_STATUS_CLOSED_PAYMENT_TIMEOUT,
                FlowTestFixtures.TOPIC_STATUS_CLOSED_DELIVERY_INACTIVE,
                FlowTestFixtures.TOPIC_STATUS_CLOSED_TIMEOUT
        })
@Import(FlowTestKafkaProducers.class)
class TransactionFlowIntegrationTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, AuctionEndedWithWinnerEvent> auctionEventKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, PaymentCreatedEvent> paymentCreatedKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, PaymentCreatedFailedEvent> paymentCreatedFailedKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, PaymentReceivedEvent> paymentReceivedKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, PaymentExpiredEvent> paymentExpiredKafkaTemplate;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionHistoryRepository transactionHistoryRepository;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private UUID correlationId;

    @BeforeEach
    void setUp() {
        transactionHistoryRepository.deleteAll();
        transactionRepository.deleteAll();
        correlationId = UUID.randomUUID();
    }

    @Test
    void shouldCompleteHappyPathFromAuctionToFinishedDelivery() throws Exception {
        sendAuctionEndedWithWinner();

        awaitStatusInDatabase(TRANSACTION_WAITING_FOR_PAYMENT);
        assertOutboundStatus(TOPIC_STATUS_WAITING, TRANSACTION_WAITING_FOR_PAYMENT);

        Long transactionId = requireTransactionId();

        sendPaymentCreated(transactionId);
        awaitStatusInDatabase(TRANSACTION_PAYMENT_PENDING);
        assertOutboundStatus(TOPIC_STATUS_PAYMENT_PENDING, TRANSACTION_PAYMENT_PENDING);

        sendPaymentReceived(transactionId);
        awaitStatusInDatabase(DELIVERY_PENDING);
        assertOutboundStatus(TOPIC_STATUS_DELIVERY_PENDING, DELIVERY_PENDING);

        mockMvc.perform(post("/transactions/{id}/confirm-delivery", transactionId)
                        .header(TransactionController.USER_ID_HEADER, FlowTestFixtures.BUYER_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent());

        awaitStatusInDatabase(TRANSACTION_FINISHED);
        assertOutboundStatus(TOPIC_STATUS_FINISHED, TRANSACTION_FINISHED);

        Transaction transaction = transactionRepository.findByCorrelationId(correlationId).orElseThrow();
        assertThat(transaction.getStatus().isFinal()).isTrue();
        assertThat(transactionRepository.count()).isEqualTo(1);
        FlowTestAssertions.assertBuyerAndSeller(transaction);
    }

    @Test
    void shouldCloseWithoutPunishmentWhenPaymentCreatedFails() {
        sendAuctionEndedWithWinner();
        awaitStatusInDatabase(TRANSACTION_WAITING_FOR_PAYMENT);

        Long transactionId = requireTransactionId();
        sendPaymentCreatedFailed(transactionId);

        awaitStatusInDatabase(TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED);

        TransactionStatusEvent event =
                assertOutboundStatus(TOPIC_STATUS_CLOSED_PAYMENT_CREATED_FAILED,
                        TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED);
        assertThat(event.sellerId()).isEqualTo(FlowTestFixtures.SELLER_ID);
        assertThat(event.highestBidderId()).isEqualTo(FlowTestFixtures.BUYER_ID);

        Transaction transaction = transactionRepository.findByCorrelationId(correlationId).orElseThrow();
        assertThat(transaction.getStatus().isFinal()).isTrue();
        assertThat(transactionRepository.count()).isEqualTo(1);
    }

    @Test
    void shouldCloseWithPunishmentWhenPaymentExpires() {
        sendAuctionEndedWithWinner();
        awaitStatusInDatabase(TRANSACTION_WAITING_FOR_PAYMENT);

        Long transactionId = requireTransactionId();
        sendPaymentCreated(transactionId);
        awaitStatusInDatabase(TRANSACTION_PAYMENT_PENDING);

        sendPaymentExpired(transactionId);
        awaitStatusInDatabase(TRANSACTION_CLOSED_PAYMENT_FAILED);

        TransactionStatusEvent event =
                assertOutboundStatus(TOPIC_STATUS_CLOSED_PAYMENT_FAILED, TRANSACTION_CLOSED_PAYMENT_FAILED);
        FlowTestAssertions.assertPunishmentPayload(event);

        Transaction transaction = transactionRepository.findByCorrelationId(correlationId).orElseThrow();
        assertThat(transaction.getStatus().isFinal()).isTrue();
        assertThat(transactionRepository.count()).isEqualTo(1);
    }

    @Test
    void shouldCloseByDeliveryInactiveAfterSevenDayTimeout() {
        sendAuctionEndedWithWinner();
        awaitStatusInDatabase(TRANSACTION_WAITING_FOR_PAYMENT);

        Long transactionId = requireTransactionId();
        sendPaymentCreated(transactionId);
        awaitStatusInDatabase(TRANSACTION_PAYMENT_PENDING);

        sendPaymentReceived(transactionId);
        awaitStatusInDatabase(DELIVERY_PENDING);

        Transaction transaction = transactionRepository.findByCorrelationId(correlationId).orElseThrow();
        transaction.setExpiresAt(LocalDateTime.now(ZoneOffset.UTC).minusDays(1));
        transactionRepository.save(transaction);

        transactionService.closeByDeliveryInactive();

        awaitStatusInDatabase(TRANSACTION_CLOSED_DELIVERY_INACTIVE);
        assertOutboundStatus(TOPIC_STATUS_CLOSED_DELIVERY_INACTIVE, TRANSACTION_CLOSED_DELIVERY_INACTIVE);

        Transaction closed = transactionRepository.findByCorrelationId(correlationId).orElseThrow();
        assertThat(closed.getStatus().isFinal()).isTrue();
        assertThat(transactionRepository.count()).isEqualTo(1);
    }

    private void sendAuctionEndedWithWinner() {
        auctionEventKafkaTemplate.send(
                TOPIC_AUCTION,
                correlationId.toString(),
                FlowTestFixtures.auctionEndedWithWinner(correlationId));
    }

    private void sendPaymentCreated(Long transactionId) {
        paymentCreatedKafkaTemplate.send(
                TOPIC_PAYMENT_CREATED,
                correlationId.toString(),
                FlowTestFixtures.paymentCreated(correlationId, transactionId));
    }

    private void sendPaymentCreatedFailed(Long transactionId) {
        paymentCreatedFailedKafkaTemplate.send(
                TOPIC_PAYMENT_CREATED_FAILED,
                correlationId.toString(),
                FlowTestFixtures.paymentCreatedFailed(correlationId, transactionId));
    }

    private void sendPaymentReceived(Long transactionId) {
        paymentReceivedKafkaTemplate.send(
                TOPIC_PAYMENT_RECEIVED,
                correlationId.toString(),
                FlowTestFixtures.paymentReceived(correlationId, transactionId));
    }

    private void sendPaymentExpired(Long transactionId) {
        paymentExpiredKafkaTemplate.send(
                TOPIC_PAYMENT_EXPIRED,
                correlationId.toString(),
                FlowTestFixtures.paymentExpired(correlationId, transactionId));
    }

    private void awaitStatusInDatabase(TransactionStatus expectedStatus) {
        await().atMost(AWAIT_TIMEOUT).untilAsserted(() ->
                FlowTestAssertions.assertSingleTransactionWithStatus(
                        transactionRepository, correlationId, expectedStatus));
    }

    private Long requireTransactionId() {
        return transactionRepository.findByCorrelationId(correlationId)
                .map(Transaction::getId)
                .orElseThrow(() -> new IllegalStateException("Transação não encontrada"));
    }

    private TransactionStatusEvent assertOutboundStatus(String topic, TransactionStatus expectedStatus) {
        TransactionStatusEvent event = await().atMost(AWAIT_TIMEOUT).until(
                () -> FlowTestOutboundCapture.awaitStatusEvent(
                        embeddedKafka, topic, correlationId, expectedStatus, Duration.ofSeconds(1)),
                java.util.Objects::nonNull);
        assertThat(event.status()).isEqualTo(expectedStatus);
        return event;
    }
}
