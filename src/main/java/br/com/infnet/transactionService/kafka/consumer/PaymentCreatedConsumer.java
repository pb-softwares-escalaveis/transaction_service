package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentCreatedConsumer {

    private final TransactionService transactionService;

    @KafkaListener(
            topics = "payments.payment.created",
            groupId = "transaction-service",
            containerFactory = "paymentCreatedKafkaListenerContainerFactory")
    public void consume(PaymentCreatedEvent event) {
        log.info("Recebido PaymentCreated: correlationId={}, transactionId={}",
                event.correlationId(), event.transactionId());
        try {
            transactionService.handlePaymentCreated(event);
        } catch (TransactionNotFoundException ex) {
            log.warn("Transação não encontrada para PaymentCreated: transactionId={}", event.transactionId());
        }
    }
}
