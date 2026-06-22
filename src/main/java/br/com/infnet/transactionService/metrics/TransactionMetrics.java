package br.com.infnet.transactionService.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class TransactionMetrics {

    private final MeterRegistry meterRegistry;
    private final Timer transitionTimer;

    public TransactionMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.transitionTimer = Timer.builder("transaction_transition_seconds")
                .description("Tempo gasto em transições de status")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    public void incrementStatusTransition(String fromStatus, String toStatus) {
        meterRegistry.counter(
                "transaction_status_transitions_total",
                "from_status", fromStatus,
                "to_status", toStatus
        ).increment();
    }

    public void incrementEventPublished(String eventType, String topic) {
        meterRegistry.counter(
                "transaction_events_published_total",
                "event_type", eventType,
                "topic", topic
        ).increment();
    }

    public void incrementTimeoutClosed(String reason) {
        meterRegistry.counter(
                "transaction_timeouts_closed_total",
                "reason", reason
        ).increment();
    }

    public void incrementRestRequest(String endpoint, String statusCode) {
        meterRegistry.counter(
                "transaction_rest_requests_total",
                "endpoint", endpoint,
                "status_code", statusCode
        ).increment();
    }

    public void recordTransition(Runnable runnable) {
        transitionTimer.record(runnable);
    }
}
