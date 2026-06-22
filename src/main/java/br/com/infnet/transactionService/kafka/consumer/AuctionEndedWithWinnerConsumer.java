package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
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
public class AuctionEndedWithWinnerConsumer {

    private static final String EVENT_TYPE = "auction_ended_with_winner";

    private final TransactionService transactionService;
    private final TransactionConsumerMetrics consumerMetrics;

    @KafkaListener(
            topics = "auctions.lot.ended-with-winner",
            groupId = "transaction-service",
            containerFactory = "auctionEndedWithWinnerKafkaListenerContainerFactory")
    public void consume(AuctionEndedWithWinnerEvent event) {
        MDC.put("correlationId", event.correlationId().toString());
        consumerMetrics.incrementConsumed(EVENT_TYPE);

        try {
            log.info("Recebido AuctionEndedWithWinner: correlationId={}, auctionId={}",
                    event.correlationId(), event.auctionId());
            transactionService.handleAuctionEndedWithWinner(event);
            consumerMetrics.incrementProcessed(EVENT_TYPE);
            log.info("AuctionEndedWithWinner processado: correlationId={}", event.correlationId());
        } catch (Exception ex) {
            consumerMetrics.incrementFailed(EVENT_TYPE);
            log.error("Falha ao processar AuctionEndedWithWinner: correlationId={}",
                    event.correlationId(), ex);
            throw ex;
        } finally {
            MDC.remove("correlationId");
        }
    }
}
