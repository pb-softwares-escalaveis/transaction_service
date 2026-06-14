package br.com.infnet.transactionService.integration;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class FlowTestFixtures {

    public static final Long AUCTION_ID = 100L;
    public static final UUID SELLER_ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-111111111111");
    public static final UUID BUYER_ID = UUID.fromString("bbbbbbbb-cccc-dddd-eeee-222222222222");
    public static final UUID PAYMENT_ID = UUID.fromString("cccccccc-dddd-eeee-ffff-333333333333");
    public static final BigDecimal WINNER_BID = new BigDecimal("250.50");
    public static final Instant OCCURRED_AT = Instant.parse("2026-06-14T10:00:00Z");

    public static final String TOPIC_AUCTION = "auctions.lot.ended-with-winner";
    public static final String TOPIC_PAYMENT_CREATED = "payments.payment.created";
    public static final String TOPIC_PAYMENT_CREATED_FAILED = "payments.payment.created.failed";
    public static final String TOPIC_PAYMENT_RECEIVED = "payments.payment.received";
    public static final String TOPIC_PAYMENT_EXPIRED = "payments.payment.expired";

    public static final String TOPIC_PAYMENT_REQUESTED = "transactions.payment.requested";
    public static final String TOPIC_STATUS_CREATED = "transactions.status.created";
    public static final String TOPIC_STATUS_WAITING = "transactions.status.waiting-for-payment";
    public static final String TOPIC_STATUS_PAYMENT_PENDING = "transactions.status.payment-pending";
    public static final String TOPIC_STATUS_CLOSED_PAYMENT_CREATED_FAILED =
            "transactions.status.closed.payment-created-failed";
    public static final String TOPIC_STATUS_DELIVERY_PENDING = "transactions.status.delivery-pending";
    public static final String TOPIC_STATUS_FINISHED = "transactions.status.finished";
    public static final String TOPIC_STATUS_CLOSED_PAYMENT_FAILED = "transactions.status.closed.payment-failed";
    public static final String TOPIC_STATUS_CLOSED_PAYMENT_TIMEOUT = "transactions.status.closed.payment-timeout";
    public static final String TOPIC_STATUS_CLOSED_DELIVERY_INACTIVE =
            "transactions.status.closed.delivery-inactive";
    public static final String TOPIC_STATUS_CLOSED_TIMEOUT = "transactions.status.closed.timeout";

    public static final String[] ALL_TOPICS = {
            TOPIC_AUCTION,
            TOPIC_PAYMENT_CREATED,
            TOPIC_PAYMENT_CREATED_FAILED,
            TOPIC_PAYMENT_RECEIVED,
            TOPIC_PAYMENT_EXPIRED,
            TOPIC_PAYMENT_REQUESTED,
            TOPIC_STATUS_CREATED,
            TOPIC_STATUS_WAITING,
            TOPIC_STATUS_PAYMENT_PENDING,
            TOPIC_STATUS_CLOSED_PAYMENT_CREATED_FAILED,
            TOPIC_STATUS_DELIVERY_PENDING,
            TOPIC_STATUS_FINISHED,
            TOPIC_STATUS_CLOSED_PAYMENT_FAILED,
            TOPIC_STATUS_CLOSED_PAYMENT_TIMEOUT,
            TOPIC_STATUS_CLOSED_DELIVERY_INACTIVE,
            TOPIC_STATUS_CLOSED_TIMEOUT
    };

    private FlowTestFixtures() {
    }

    public static AuctionEndedWithWinnerEvent auctionEndedWithWinner(UUID correlationId) {
        return new AuctionEndedWithWinnerEvent(
                correlationId,
                AUCTION_ID,
                SELLER_ID,
                BUYER_ID,
                WINNER_BID,
                OCCURRED_AT,
                "Leilão Teste",
                "https://example.com/thumb.jpg");
    }

    public static PaymentCreatedEvent paymentCreated(UUID correlationId, Long transactionId) {
        return new PaymentCreatedEvent(
                correlationId,
                transactionId,
                PAYMENT_ID,
                25050,
                "PENDING");
    }

    public static PaymentCreatedFailedEvent paymentCreatedFailed(UUID correlationId, Long transactionId) {
        return new PaymentCreatedFailedEvent(
                correlationId,
                transactionId,
                "Falha técnica ao gerar cobrança");
    }

    public static PaymentReceivedEvent paymentReceived(UUID correlationId, Long transactionId) {
        return new PaymentReceivedEvent(
                correlationId,
                BUYER_ID,
                PAYMENT_ID,
                transactionId,
                AUCTION_ID,
                OCCURRED_AT);
    }

    public static PaymentExpiredEvent paymentExpired(UUID correlationId, Long transactionId) {
        return new PaymentExpiredEvent(
                correlationId,
                BUYER_ID,
                PAYMENT_ID,
                transactionId,
                AUCTION_ID,
                OCCURRED_AT);
    }
}
