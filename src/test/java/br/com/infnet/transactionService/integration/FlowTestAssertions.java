package br.com.infnet.transactionService.integration;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import br.com.infnet.transactionService.repository.TransactionRepository;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public final class FlowTestAssertions {

    private FlowTestAssertions() {
    }

    public static void assertSingleTransactionWithStatus(
            TransactionRepository repository,
            UUID correlationId,
            TransactionStatus expectedStatus) {

        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.findByCorrelationId(correlationId))
                .isPresent()
                .get()
                .satisfies(transaction -> {
                    assertThat(transaction.getStatus()).isEqualTo(expectedStatus);
                    if (expectedStatus.isFinal()) {
                        assertThat(transaction.getStatus().isFinal()).isTrue();
                    }
                });
    }

    public static void assertPunishmentPayload(TransactionStatusEvent event) {
        assertThat(event.sellerId()).isEqualTo(FlowTestFixtures.SELLER_ID);
        assertThat(event.highestBidderId()).isEqualTo(FlowTestFixtures.BUYER_ID);
        assertThat(event.status()).isEqualTo(TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED);
    }

    public static void assertBuyerAndSeller(Transaction transaction) {
        assertThat(transaction.getSellerId()).isEqualTo(FlowTestFixtures.SELLER_ID);
        assertThat(transaction.getBuyerId()).isEqualTo(FlowTestFixtures.BUYER_ID);
    }
}
