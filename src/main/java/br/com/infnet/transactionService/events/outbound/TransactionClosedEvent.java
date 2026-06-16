package br.com.infnet.transactionService.events.outbound;

import br.com.infnet.transactionService.enums.TransactionStatus;

import java.time.Instant;
import java.util.UUID;

public record TransactionClosedEvent(
        UUID correlationId,
        Long transactionId,
        Long auctionId,
        UUID sellerId,
        UUID highestBidderId,
        TransactionStatus status,
        String reason,
        Instant occurredAt,
        Boolean penalty
) {
}
