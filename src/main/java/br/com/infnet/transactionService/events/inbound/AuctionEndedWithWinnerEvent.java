package br.com.infnet.transactionService.events.inbound;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record AuctionEndedWithWinnerEvent(
        UUID correlationId,
        Long auctionId,
        UUID sellerId,
        UUID highestBidderId,
        BigDecimal winnerBidValue,
        @JsonAlias("ocurredAt") Instant occurredAt,
        String auctionTitle,
        String auctionThumb
) {
}
