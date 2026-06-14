package br.com.infnet.transactionService.repository;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {

    Optional<Transaction> findByCorrelationId(UUID correlationId);

    boolean existsByCorrelationId(UUID correlationId);

    List<Transaction> findByStatusInAndExpiresAtBefore(
            Collection<TransactionStatus> statuses, LocalDateTime expiresAt);

    @Query("""
            SELECT t FROM Transaction t
            WHERE t.status NOT IN (
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED,
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED,
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED,
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT,
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE,
                br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT)
            AND t.createdAt < :cutoff
            """)
    List<Transaction> findNonFinalByCreatedAtBefore(@Param("cutoff") LocalDateTime cutoff);
}
