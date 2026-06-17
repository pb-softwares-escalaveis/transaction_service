package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentCreatedFailedConsumer {

    private final TransactionService transactionService;

    @KafkaListener(
            topics = "payments.payment.created-failed",
            groupId = "transaction-service",
            containerFactory = "paymentCreatedFailedKafkaListenerContainerFactory")
    public void consume(PaymentCreatedFailedEvent event) {
        log.info("Recebido PaymentCreatedFailed: correlationId={}, transactionId={}",
                event.correlationId(), event.transactionId());
        try {
            transactionService.handlePaymentCreatedFailed(event);
        } catch (TransactionNotFoundException ex) {
            log.warn("Transação não encontrada para PaymentCreatedFailed: transactionId={}", event.transactionId());
        }
    }
}
