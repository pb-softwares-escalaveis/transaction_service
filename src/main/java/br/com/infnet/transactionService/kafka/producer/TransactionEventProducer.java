package br.com.infnet.transactionService.kafka.producer;

import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.outbound.PaymentRequestedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionClosedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventProducer {

    private static final String PAYMENT_REQUESTED_TOPIC = "transactions.payment.requested";
    private static final String TRANSACTION_CLOSED_TOPIC = "transactions.status.closed";

    private final KafkaTemplate<String, PaymentRequestedEvent> paymentRequestedKafkaTemplate;
    private final KafkaTemplate<String, TransactionStatusEvent> transactionStatusKafkaTemplate;
    private final KafkaTemplate<String, TransactionClosedEvent> transactionClosedKafkaTemplate;

    public void publishPaymentRequested(PaymentRequestedEvent event) {
        String key = event.correlationId().toString();
        paymentRequestedKafkaTemplate.send(PAYMENT_REQUESTED_TOPIC, key, event);
        log.info("Publicado PaymentRequested: correlationId={}, transactionId={}",
                event.correlationId(), event.transactionId());
    }

    public void publishStatusEvent(TransactionStatusEvent event) {
        String key = event.correlationId().toString();
        String topic = resolveTopic(event.status());
        transactionStatusKafkaTemplate.send(topic, key, event);
        log.info("Publicado {} no tópico {}: correlationId={}, transactionId={}",
                event.status(), topic, event.correlationId(), event.transactionId());
    }

    public void publishClosedEvent(TransactionClosedEvent event) {
        String key = event.correlationId().toString();
        transactionClosedKafkaTemplate.send(TRANSACTION_CLOSED_TOPIC, key, event);
        log.info("Publicado {} no tópico {}: correlationId={}, transactionId={}",
                event.status(), TRANSACTION_CLOSED_TOPIC, event.correlationId(), event.transactionId());
    }

    private String resolveTopic(TransactionStatus status) {
        return switch (status) {
            case TRANSACTION_CREATED -> "transactions.status.created";
            case TRANSACTION_WAITING_FOR_PAYMENT -> "transactions.status.waiting-for-payment";
            case TRANSACTION_PAYMENT_PENDING -> "transactions.status.payment-pending";
            case DELIVERY_PENDING -> "transactions.status.delivery-pending";
            case TRANSACTION_FINISHED -> "transactions.status.finished";
            default -> throw new IllegalArgumentException("Status não possui tópico de status: " + status);
        };
    }
}
