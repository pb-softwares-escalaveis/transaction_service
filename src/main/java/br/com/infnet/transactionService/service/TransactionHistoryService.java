package br.com.infnet.transactionService.service;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.domain.TransactionHistory;
import br.com.infnet.transactionService.enums.ChangedBy;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.repository.TransactionHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class TransactionHistoryService {

    private final TransactionHistoryRepository transactionHistoryRepository;

    public TransactionHistory recordTransition(
            Transaction transaction,
            TransactionStatus oldStatus,
            TransactionStatus newStatus,
            ChangedBy changedBy,
            String reason,
            LocalDateTime occurredAt) {

        TransactionHistory history = TransactionHistory.builder()
                .transaction(transaction)
                .oldStatus(oldStatus)
                .newStatus(newStatus)
                .changedBy(changedBy)
                .reason(reason)
                .occurredAt(occurredAt)
                .build();

        return transactionHistoryRepository.save(history);
    }
}
