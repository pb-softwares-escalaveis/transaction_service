package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.metrics.TransactionConsumerMetrics;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentExpiredConsumer {

    private static final String EVENT_TYPE = "payment_expired";

    private final TransactionService transactionService;
    private final TransactionConsumerMetrics consumerMetrics;

    @KafkaListener(
            topics = "payments.payment.expired",
            groupId = "transaction-service",
            containerFactory = "paymentExpiredKafkaListenerContainerFactory")
    public void consume(PaymentExpiredEvent event) {
        MDC.put("correlationId", event.correlationId().toString());
        consumerMetrics.incrementConsumed(EVENT_TYPE);

        try {
            log.info("Recebido PaymentExpired: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId());
            transactionService.handlePaymentExpired(event);
            consumerMetrics.incrementProcessed(EVENT_TYPE);
            log.info("PaymentExpired processado: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId());
        } catch (TransactionNotFoundException ex) {
            consumerMetrics.incrementProcessed(EVENT_TYPE);
            log.warn("Transação não encontrada para PaymentExpired: transactionId={}", event.transactionId());
        } catch (Exception ex) {
            consumerMetrics.incrementFailed(EVENT_TYPE);
            log.error("Falha ao processar PaymentExpired: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId(), ex);
            throw ex;
        } finally {
            MDC.remove("correlationId");
        }
    }
}
