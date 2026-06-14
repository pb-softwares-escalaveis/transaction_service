package br.com.infnet.transactionService.service;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.exception.InvalidStateTransitionException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

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

@Component
public class TransactionStateMachine {

    private static final Map<TransactionStatus, Set<TransactionStatus>> ALLOWED_TRANSITIONS;

    static {
        Map<TransactionStatus, Set<TransactionStatus>> transitions = new EnumMap<>(TransactionStatus.class);

        transitions.put(TRANSACTION_CREATED, Set.of(
                TRANSACTION_WAITING_FOR_PAYMENT,
                TRANSACTION_CLOSED_TIMEOUT));

        transitions.put(TRANSACTION_WAITING_FOR_PAYMENT, Set.of(
                TRANSACTION_PAYMENT_PENDING,
                TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED,
                TRANSACTION_CLOSED_PAYMENT_TIMEOUT,
                TRANSACTION_CLOSED_TIMEOUT));

        transitions.put(TRANSACTION_PAYMENT_PENDING, Set.of(
                DELIVERY_PENDING,
                TRANSACTION_CLOSED_PAYMENT_FAILED,
                TRANSACTION_CLOSED_PAYMENT_TIMEOUT,
                TRANSACTION_CLOSED_TIMEOUT));

        transitions.put(DELIVERY_PENDING, Set.of(
                TRANSACTION_FINISHED,
                TRANSACTION_CLOSED_DELIVERY_INACTIVE,
                TRANSACTION_CLOSED_TIMEOUT));

        ALLOWED_TRANSITIONS = Map.copyOf(transitions);
    }

    public boolean canTransition(TransactionStatus from, TransactionStatus to) {
        if (from.isFinal()) {
            return false;
        }
        return ALLOWED_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }

    public void applyTransition(Transaction transaction, TransactionStatus newStatus, LocalDateTime now) {
        TransactionStatus currentStatus = transaction.getStatus();
        if (!canTransition(currentStatus, newStatus)) {
            throw new InvalidStateTransitionException(currentStatus, newStatus);
        }

        transaction.setStatus(newStatus);
        transaction.setUpdatedAt(now);
        updateExpiresAt(transaction, newStatus, now);
    }

    private void updateExpiresAt(Transaction transaction, TransactionStatus newStatus, LocalDateTime now) {
        switch (newStatus) {
            case TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_PAYMENT_PENDING ->
                    transaction.setExpiresAt(now.plusHours(24));
            case DELIVERY_PENDING ->
                    transaction.setExpiresAt(now.plusDays(7));
            default -> {
                // Estados finais mantêm o último expires_at
            }
        }
    }
}
