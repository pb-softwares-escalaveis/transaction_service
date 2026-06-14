package br.com.infnet.transactionService.events.inbound;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentReceivedEvent(
        UUID correlationId,
        UUID bidderId,
        UUID paymentId,
        Long transactionId,
        Long auctionId,
        Instant occurredAt
) {
}
