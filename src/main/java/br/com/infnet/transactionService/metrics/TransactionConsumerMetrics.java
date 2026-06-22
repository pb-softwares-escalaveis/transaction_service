package br.com.infnet.transactionService.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TransactionConsumerMetrics {

    private final MeterRegistry meterRegistry;

    public void incrementConsumed(String eventType) {
        meterRegistry.counter(
                "transaction_messages_consumed_total",
                "event_type", eventType
        ).increment();
    }

    public void incrementProcessed(String eventType) {
        meterRegistry.counter(
                "transaction_messages_processed_total",
                "event_type", eventType
        ).increment();
    }

    public void incrementFailed(String eventType) {
        meterRegistry.counter(
                "transaction_messages_failed_total",
                "event_type", eventType
        ).increment();
    }
}
