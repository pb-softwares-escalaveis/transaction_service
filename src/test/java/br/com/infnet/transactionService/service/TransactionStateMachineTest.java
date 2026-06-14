package br.com.infnet.transactionService.service;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.exception.InvalidStateTransitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.stream.Stream;

import static br.com.infnet.transactionService.enums.TransactionStatus.DELIVERY_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CREATED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_PAYMENT_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_WAITING_FOR_PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionStateMachineTest {

    private static final LocalDateTime FIXED_NOW = LocalDateTime.of(2026, 6, 14, 10, 0, 0);

    private TransactionStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new TransactionStateMachine();
    }

    @ParameterizedTest(name = "{0} → {1}")
    @MethodSource("validTransitions")
    void shouldAllowValidTransition(TransactionStatus from, TransactionStatus to) {
        Transaction transaction = transactionWithStatus(from);

        stateMachine.applyTransition(transaction, to, FIXED_NOW);

        assertThat(transaction.getStatus()).isEqualTo(to);
        assertThat(transaction.getUpdatedAt()).isEqualTo(FIXED_NOW);
    }

    static Stream<Arguments> validTransitions() {
        return Stream.of(
                Arguments.of(TRANSACTION_CREATED, TRANSACTION_WAITING_FOR_PAYMENT),
                Arguments.of(TRANSACTION_CREATED, TRANSACTION_CLOSED_TIMEOUT),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_PAYMENT_PENDING),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_CLOSED_PAYMENT_TIMEOUT),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_CLOSED_TIMEOUT),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, DELIVERY_PENDING),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, TRANSACTION_CLOSED_PAYMENT_FAILED),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, TRANSACTION_CLOSED_PAYMENT_TIMEOUT),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, TRANSACTION_CLOSED_TIMEOUT),
                Arguments.of(DELIVERY_PENDING, TRANSACTION_FINISHED),
                Arguments.of(DELIVERY_PENDING, TRANSACTION_CLOSED_DELIVERY_INACTIVE),
                Arguments.of(DELIVERY_PENDING, TRANSACTION_CLOSED_TIMEOUT));
    }

    @ParameterizedTest(name = "bloqueia {0} → {1}")
    @MethodSource("invalidTransitions")
    void shouldRejectInvalidTransition(TransactionStatus from, TransactionStatus to) {
        Transaction transaction = transactionWithStatus(from);

        assertThat(stateMachine.canTransition(from, to)).isFalse();
        assertThatThrownBy(() -> stateMachine.applyTransition(transaction, to, FIXED_NOW))
                .isInstanceOf(InvalidStateTransitionException.class)
                .hasMessageContaining(from.name())
                .hasMessageContaining(to.name());
        assertThat(transaction.getStatus()).isEqualTo(from);
    }

    static Stream<Arguments> invalidTransitions() {
        return Stream.of(
                Arguments.of(TRANSACTION_CREATED, TRANSACTION_PAYMENT_PENDING),
                Arguments.of(TRANSACTION_CREATED, DELIVERY_PENDING),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, DELIVERY_PENDING),
                Arguments.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_FINISHED),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, TRANSACTION_WAITING_FOR_PAYMENT),
                Arguments.of(TRANSACTION_PAYMENT_PENDING, TRANSACTION_FINISHED),
                Arguments.of(DELIVERY_PENDING, TRANSACTION_PAYMENT_PENDING),
                Arguments.of(DELIVERY_PENDING, TRANSACTION_CLOSED_PAYMENT_FAILED));
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, mode = EnumSource.Mode.INCLUDE, names = {
            "TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED",
            "TRANSACTION_FINISHED",
            "TRANSACTION_CLOSED_PAYMENT_FAILED",
            "TRANSACTION_CLOSED_PAYMENT_TIMEOUT",
            "TRANSACTION_CLOSED_DELIVERY_INACTIVE",
            "TRANSACTION_CLOSED_TIMEOUT"
    })
    void shouldBlockTransitionFromFinalStates(TransactionStatus finalStatus) {
        Transaction transaction = transactionWithStatus(finalStatus);

        assertThat(stateMachine.canTransition(finalStatus, TRANSACTION_WAITING_FOR_PAYMENT)).isFalse();
        assertThatThrownBy(() ->
                stateMachine.applyTransition(transaction, TRANSACTION_WAITING_FOR_PAYMENT, FIXED_NOW))
                .isInstanceOf(InvalidStateTransitionException.class);
        assertThat(transaction.getStatus()).isEqualTo(finalStatus);
    }

    @Test
    void shouldSetExpiresAtPlus24HoursWhenEnteringWaitingForPayment() {
        Transaction transaction = transactionWithStatus(TRANSACTION_CREATED);

        stateMachine.applyTransition(transaction, TRANSACTION_WAITING_FOR_PAYMENT, FIXED_NOW);

        assertThat(transaction.getExpiresAt()).isEqualTo(FIXED_NOW.plusHours(24));
    }

    @Test
    void shouldSetExpiresAtPlus24HoursWhenEnteringPaymentPending() {
        Transaction transaction = transactionWithStatus(TRANSACTION_WAITING_FOR_PAYMENT);

        stateMachine.applyTransition(transaction, TRANSACTION_PAYMENT_PENDING, FIXED_NOW);

        assertThat(transaction.getExpiresAt()).isEqualTo(FIXED_NOW.plusHours(24));
    }

    @Test
    void shouldSetExpiresAtPlus7DaysWhenEnteringDeliveryPending() {
        Transaction transaction = transactionWithStatus(TRANSACTION_PAYMENT_PENDING);

        stateMachine.applyTransition(transaction, DELIVERY_PENDING, FIXED_NOW);

        assertThat(transaction.getExpiresAt()).isEqualTo(FIXED_NOW.plusDays(7));
    }

    @Test
    void shouldNotChangeExpiresAtWhenEnteringFinalState() {
        LocalDateTime previousExpiresAt = FIXED_NOW.plusHours(12);
        Transaction transaction = transactionWithStatus(TRANSACTION_PAYMENT_PENDING);
        transaction.setExpiresAt(previousExpiresAt);

        stateMachine.applyTransition(transaction, TRANSACTION_CLOSED_PAYMENT_FAILED, FIXED_NOW);

        assertThat(transaction.getExpiresAt()).isEqualTo(previousExpiresAt);
    }

    private Transaction transactionWithStatus(TransactionStatus status) {
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
                .expiresAt(FIXED_NOW.plusHours(1))
                .build();
    }
}
