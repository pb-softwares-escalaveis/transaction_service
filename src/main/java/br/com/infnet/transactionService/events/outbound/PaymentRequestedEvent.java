package br.com.infnet.transactionService.events.outbound;

import java.util.UUID;

public record PaymentRequestedEvent(
        UUID correlationId,
        Long auctionId,
        Long transactionId,
        UUID highestBidderId,
        Integer amountInCents,
        Integer expiresInSeconds
) {

    public static final int DEFAULT_EXPIRES_IN_SECONDS = 86400;
}
