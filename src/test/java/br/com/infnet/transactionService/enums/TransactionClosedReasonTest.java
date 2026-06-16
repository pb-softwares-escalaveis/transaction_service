package br.com.infnet.transactionService.enums;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionClosedReasonTest {

    @Test
    void shouldMapAllClosedStatuses() {
        assertThat(TransactionClosedReason.fromStatus(TRANSACTION_CLOSED_DELIVERY_INACTIVE))
                .isEqualTo(TransactionClosedReason.DELIVERY_INACTIVE);
        assertThat(TransactionClosedReason.fromStatus(TRANSACTION_CLOSED_PAYMENT_FAILED))
                .isEqualTo(TransactionClosedReason.PAYMENT_FAILED);
        assertThat(TransactionClosedReason.fromStatus(TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED))
                .isEqualTo(TransactionClosedReason.PAYMENT_CREATED_FAILED);
        assertThat(TransactionClosedReason.fromStatus(TRANSACTION_CLOSED_PAYMENT_TIMEOUT))
                .isEqualTo(TransactionClosedReason.PAYMENT_TIMEOUT);
        assertThat(TransactionClosedReason.fromStatus(TRANSACTION_CLOSED_TIMEOUT))
                .isEqualTo(TransactionClosedReason.TIMEOUT);
    }

    @Test
    void shouldApplyPenaltyOnlyForPaymentFailed() {
        assertThat(TransactionClosedReason.PAYMENT_FAILED.penalty()).isTrue();

        assertThat(TransactionClosedReason.DELIVERY_INACTIVE.penalty()).isFalse();
        assertThat(TransactionClosedReason.PAYMENT_CREATED_FAILED.penalty()).isFalse();
        assertThat(TransactionClosedReason.PAYMENT_TIMEOUT.penalty()).isFalse();
        assertThat(TransactionClosedReason.TIMEOUT.penalty()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(TransactionClosedReason.class)
    void shouldExposeNonBlankPortugueseMessages(TransactionClosedReason reason) {
        assertThat(reason.message()).isNotBlank();
        assertThat(reason.status()).isNotNull();
    }

    @Test
    void shouldRejectNonClosedStatus() {
        assertThatThrownBy(() -> TransactionClosedReason.fromStatus(TRANSACTION_FINISHED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TRANSACTION_FINISHED");
    }
}
