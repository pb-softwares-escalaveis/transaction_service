package br.com.infnet.transactionService.events.inbound;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentCreatedFailedEvent(
        UUID correlationId,
        Long transactionId,
        String reason
) {
}
