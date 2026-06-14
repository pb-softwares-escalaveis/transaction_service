package br.com.infnet.transactionService.events.inbound;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentCreatedEvent(
        UUID correlationId,
        Long transactionId,
        UUID paymentId,
        Integer amountInCents,
        String status
) {
}
