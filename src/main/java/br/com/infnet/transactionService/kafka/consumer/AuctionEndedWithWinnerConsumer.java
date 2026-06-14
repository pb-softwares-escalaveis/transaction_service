package br.com.infnet.transactionService.kafka.consumer;

import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AuctionEndedWithWinnerConsumer {

    private final TransactionService transactionService;

    @KafkaListener(
            topics = "auctions.lot.ended-with-winner",
            groupId = "transaction-service",
            containerFactory = "auctionEndedWithWinnerKafkaListenerContainerFactory")
    public void consume(AuctionEndedWithWinnerEvent event) {
        log.info("Recebido AuctionEndedWithWinner: correlationId={}", event.correlationId());
        transactionService.handleAuctionEndedWithWinner(event);
    }
}
