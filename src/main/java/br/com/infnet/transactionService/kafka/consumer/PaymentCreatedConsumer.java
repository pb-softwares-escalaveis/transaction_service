package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
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
public class PaymentCreatedConsumer {

    private static final String EVENT_TYPE = "payment_created";

    private final TransactionService transactionService;
    private final TransactionConsumerMetrics consumerMetrics;

    @KafkaListener(
            topics = "payments.payment.created",
            groupId = "transaction-service",
            containerFactory = "paymentCreatedKafkaListenerContainerFactory")
    public void consume(PaymentCreatedEvent event) {
        MDC.put("correlationId", event.correlationId().toString());
        consumerMetrics.incrementConsumed(EVENT_TYPE);

        try {
            log.info("Recebido PaymentCreated: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId());
            transactionService.handlePaymentCreated(event);
            consumerMetrics.incrementProcessed(EVENT_TYPE);
            log.info("PaymentCreated processado: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId());
        } catch (TransactionNotFoundException ex) {
            consumerMetrics.incrementProcessed(EVENT_TYPE);
            log.warn("Transação não encontrada para PaymentCreated: transactionId={}", event.transactionId());
        } catch (Exception ex) {
            consumerMetrics.incrementFailed(EVENT_TYPE);
            log.error("Falha ao processar PaymentCreated: correlationId={}, transactionId={}",
                    event.correlationId(), event.transactionId(), ex);
            throw ex;
        } finally {
            MDC.remove("correlationId");
        }
    }
}
