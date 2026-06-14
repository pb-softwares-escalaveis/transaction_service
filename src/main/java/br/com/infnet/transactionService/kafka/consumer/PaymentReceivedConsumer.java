package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentReceivedConsumer {

    private final TransactionService transactionService;

    @KafkaListener(
            topics = "payments.payment.received",
            groupId = "transaction-service",
            containerFactory = "paymentReceivedKafkaListenerContainerFactory")
    public void consume(PaymentReceivedEvent event) {
        log.info("Recebido PaymentReceived: correlationId={}, transactionId={}",
                event.correlationId(), event.transactionId());
        try {
            transactionService.handlePaymentReceived(event);
        } catch (TransactionNotFoundException ex) {
            log.warn("Transação não encontrada para PaymentReceived: transactionId={}", event.transactionId());
        }
    }
}
