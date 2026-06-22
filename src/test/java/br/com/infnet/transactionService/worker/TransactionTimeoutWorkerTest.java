package br.com.infnet.transactionService.worker;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.ChangedBy;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.outbound.TransactionClosedEvent;
import br.com.infnet.transactionService.kafka.producer.TransactionEventProducer;
import br.com.infnet.transactionService.metrics.TransactionMetrics;
import br.com.infnet.transactionService.repository.TransactionRepository;
import br.com.infnet.transactionService.service.TransactionHistoryService;
import br.com.infnet.transactionService.service.TransactionService;
import br.com.infnet.transactionService.service.TransactionStateMachine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static br.com.infnet.transactionService.enums.TransactionStatus.DELIVERY_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CREATED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_PAYMENT_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_WAITING_FOR_PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionTimeoutWorkerTest {

    private static final LocalDateTime FIXED_NOW = LocalDateTime.of(2026, 6, 14, 12, 0, 0);

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private TransactionHistoryService historyService;

    @Mock
    private TransactionEventProducer eventProducer;

    private TransactionMetrics transactionMetrics;
    private TransactionService transactionService;
    private TransactionTimeoutWorker worker;

    @BeforeEach
    void setUp() {
        transactionMetrics = new TransactionMetrics(new SimpleMeterRegistry());
        transactionService = new TransactionService(
                transactionRepository,
                new TransactionStateMachine(),
                historyService,
                eventProducer,
                transactionMetrics);
        worker = new TransactionTimeoutWorker(transactionService);
    }

    @Test
    void shouldInvokeAllCloseByMethodsWhenProcessingTimeouts() {
        when(transactionRepository.findByStatusInAndExpiresAtBefore(any(), any())).thenReturn(List.of());
        when(transactionRepository.findNonFinalByCreatedAtBefore(any())).thenReturn(List.of());

        worker.processTimeouts();

        verify(transactionRepository).findByStatusInAndExpiresAtBefore(
                eq(List.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_PAYMENT_PENDING)), any());
        verify(transactionRepository).findByStatusInAndExpiresAtBefore(eq(List.of(DELIVERY_PENDING)), any());
        verify(transactionRepository).findNonFinalByCreatedAtBefore(any());
    }

    @Test
    void shouldCloseExpiredPaymentPhaseTransactionsAfter24Hours() {
        Transaction waiting = transactionWithStatus(TRANSACTION_WAITING_FOR_PAYMENT, FIXED_NOW.minusDays(1));
        Transaction pending = transactionWithStatus(TRANSACTION_PAYMENT_PENDING, FIXED_NOW.minusHours(1));

        when(transactionRepository.findByStatusInAndExpiresAtBefore(
                eq(List.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_PAYMENT_PENDING)), any()))
                .thenReturn(List.of(waiting, pending));
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));

        transactionService.closeByPaymentTimeout();

        assertThat(waiting.getStatus()).isEqualTo(TRANSACTION_CLOSED_PAYMENT_TIMEOUT);
        assertThat(pending.getStatus()).isEqualTo(TRANSACTION_CLOSED_PAYMENT_TIMEOUT);
        verify(eventProducer, org.mockito.Mockito.times(2)).publishClosedEvent(any(TransactionClosedEvent.class));
    }

    @Test
    void shouldCloseExpiredDeliveryPendingTransactionsAfter7Days() {
        Transaction deliveryPending = transactionWithStatus(DELIVERY_PENDING, FIXED_NOW.minusDays(8));

        when(transactionRepository.findByStatusInAndExpiresAtBefore(eq(List.of(DELIVERY_PENDING)), any()))
                .thenReturn(List.of(deliveryPending));
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));

        transactionService.closeByDeliveryInactive();

        assertThat(deliveryPending.getStatus()).isEqualTo(TRANSACTION_CLOSED_DELIVERY_INACTIVE);
        verify(eventProducer).publishClosedEvent(any(TransactionClosedEvent.class));
    }

    @Test
    void shouldCloseNonFinalTransactionsAfter11DaysUsingCreatedAt() {
        Transaction stale = transactionWithStatus(TRANSACTION_CREATED, FIXED_NOW.plusDays(1));
        stale.setCreatedAt(FIXED_NOW.minusDays(12));
        stale.setExpiresAt(FIXED_NOW.plusDays(1));

        when(transactionRepository.findNonFinalByCreatedAtBefore(any())).thenReturn(List.of(stale));
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));

        transactionService.closeByGlobalTimeout();

        assertThat(stale.getStatus()).isEqualTo(TRANSACTION_CLOSED_TIMEOUT);
        verify(eventProducer).publishClosedEvent(any(TransactionClosedEvent.class));
    }

    @Test
    void shouldQueryGlobalTimeoutWithElevenDayCutoff() {
        when(transactionRepository.findNonFinalByCreatedAtBefore(any())).thenReturn(List.of());

        transactionService.closeByGlobalTimeout();

        ArgumentCaptor<LocalDateTime> cutoffCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(transactionRepository).findNonFinalByCreatedAtBefore(cutoffCaptor.capture());
        assertThat(cutoffCaptor.getValue()).isBefore(LocalDateTime.now(ZoneOffset.UTC).minusDays(10).plusHours(23));
        assertThat(cutoffCaptor.getValue()).isAfter(LocalDateTime.now(ZoneOffset.UTC).minusDays(11).minusHours(1));
    }

    @Test
    void shouldDoNothingWhenNoExpiredTransactionsAreFound() {
        when(transactionRepository.findByStatusInAndExpiresAtBefore(any(), any())).thenReturn(List.of());

        transactionService.closeByPaymentTimeout();

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(eventProducer);
    }

    private Transaction transactionWithStatus(TransactionStatus status, LocalDateTime expiresAt) {
        return Transaction.builder()
                .id(1L)
                .correlationId(UUID.randomUUID())
                .auctionId(100L)
                .buyerId(UUID.randomUUID())
                .sellerId(UUID.randomUUID())
                .winnerBidValue(new BigDecimal("150.00"))
                .amountInCents(15000)
                .status(status)
                .createdAt(FIXED_NOW.minusDays(1))
                .updatedAt(FIXED_NOW.minusDays(1))
                .expiresAt(expiresAt)
                .build();
    }
}
