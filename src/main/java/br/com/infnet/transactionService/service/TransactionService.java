package br.com.infnet.transactionService.service;

import br.com.infnet.transactionService.domain.Transaction;
import br.com.infnet.transactionService.enums.ChangedBy;
import br.com.infnet.transactionService.enums.TransactionStatus;
import br.com.infnet.transactionService.events.inbound.AuctionEndedWithWinnerEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentCreatedFailedEvent;
import br.com.infnet.transactionService.events.inbound.PaymentExpiredEvent;
import br.com.infnet.transactionService.events.inbound.PaymentReceivedEvent;
import br.com.infnet.transactionService.enums.TransactionClosedReason;
import br.com.infnet.transactionService.events.outbound.PaymentRequestedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionClosedEvent;
import br.com.infnet.transactionService.events.outbound.TransactionStatusEvent;
import br.com.infnet.transactionService.dto.TransactionStatusResponse;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.exception.UnauthorizedBuyerException;
import br.com.infnet.transactionService.exception.UnauthorizedTransactionAccessException;
import br.com.infnet.transactionService.kafka.producer.TransactionEventProducer;
import br.com.infnet.transactionService.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static br.com.infnet.transactionService.enums.TransactionStatus.DELIVERY_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CREATED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_PAYMENT_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_WAITING_FOR_PAYMENT;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final TransactionStateMachine stateMachine;
    private final TransactionHistoryService historyService;
    private final TransactionEventProducer eventProducer;

    @Transactional
    public Transaction transition(
            Transaction transaction,
            TransactionStatus newStatus,
            ChangedBy changedBy,
            String reason) {

        LocalDateTime now = LocalDateTime.now();
        TransactionStatus oldStatus = transaction.getStatus();

        stateMachine.applyTransition(transaction, newStatus, now);
        historyService.recordTransition(transaction, oldStatus, newStatus, changedBy, reason, now);

        Transaction saved = transactionRepository.save(transaction);
        Instant occurredAt = toInstant(now);
        if (newStatus.isFinal() && isClosedStatus(newStatus)) {
            publishClosedEvent(saved, newStatus, occurredAt);
        } else {
            publishStatusEvent(saved, newStatus, occurredAt);
        }
        return saved;
    }

    @Transactional
    public void handleAuctionEndedWithWinner(AuctionEndedWithWinnerEvent event) {
        if (transactionRepository.existsByCorrelationId(event.correlationId())) {
            log.warn("Transação já existe para correlationId={}, ignorando evento duplicado", event.correlationId());
            return;
        }

        LocalDateTime now = toLocalDateTime(event.occurredAt());
        int amountInCents = toAmountInCents(event.winnerBidValue());

        Transaction transaction = Transaction.builder()
                .correlationId(event.correlationId())
                .auctionId(event.auctionId())
                .buyerId(event.highestBidderId())
                .sellerId(event.sellerId())
                .winnerBidValue(event.winnerBidValue())
                .amountInCents(amountInCents)
                .status(TRANSACTION_CREATED)
                .createdAt(now)
                .updatedAt(now)
                .expiresAt(now.plusHours(24))
                .build();

        try {
            transaction = transactionRepository.save(transaction);
        } catch (DataIntegrityViolationException ex) {
            log.warn("Violação de UNIQUE em correlationId={}, ignorando evento duplicado", event.correlationId());
            return;
        }

        Instant occurredAt = toInstant(event.occurredAt());
        publishStatusEvent(transaction, TRANSACTION_CREATED, occurredAt);
        publishPaymentRequested(transaction);
        transition(transaction, TRANSACTION_WAITING_FOR_PAYMENT, ChangedBy.SYSTEM, null);
    }

    @Transactional
    public void handlePaymentCreated(PaymentCreatedEvent event) {
        Transaction transaction = findTransactionOrThrow(event.transactionId());

        if (isAlreadyProcessed(transaction, TRANSACTION_PAYMENT_PENDING, event.correlationId())) {
            return;
        }
        if (!isExpectedStatus(transaction, TRANSACTION_WAITING_FOR_PAYMENT, event.correlationId())) {
            return;
        }

        transaction.setPaymentId(event.paymentId());
        transition(transaction, TRANSACTION_PAYMENT_PENDING, ChangedBy.SYSTEM, null);
    }

    @Transactional
    public void handlePaymentCreatedFailed(PaymentCreatedFailedEvent event) {
        Transaction transaction = findTransactionOrThrow(event.transactionId());

        if (isAlreadyProcessed(transaction, TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED, event.correlationId())) {
            return;
        }
        if (!isExpectedStatus(transaction, TRANSACTION_WAITING_FOR_PAYMENT, event.correlationId())) {
            return;
        }

        transition(transaction, TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED, ChangedBy.SYSTEM, event.reason());
    }

    @Transactional
    public void handlePaymentReceived(PaymentReceivedEvent event) {
        Transaction transaction = findTransactionOrThrow(event.transactionId());

        if (isAlreadyProcessed(transaction, DELIVERY_PENDING, event.correlationId())) {
            return;
        }
        if (!isExpectedStatus(transaction, TRANSACTION_PAYMENT_PENDING, event.correlationId())) {
            return;
        }

        transition(transaction, DELIVERY_PENDING, ChangedBy.SYSTEM, null);
    }

    @Transactional
    public void confirmDelivery(Long transactionId, UUID userId) {
        Transaction transaction = findTransactionOrThrow(transactionId);

        if (!transaction.getBuyerId().equals(userId)) {
            throw new UnauthorizedBuyerException();
        }

        transition(transaction, TRANSACTION_FINISHED, ChangedBy.USER, "Entrega confirmada pelo comprador");
    }

    @Transactional(readOnly = true)
    public TransactionStatusResponse getStatus(Long transactionId, UUID userId) {
        Transaction transaction = findTransactionOrThrow(transactionId);
        assertParticipant(transaction, userId);
        return new TransactionStatusResponse(transaction.getId(), transaction.getStatus());
    }

    @Transactional
    public void handlePaymentExpired(PaymentExpiredEvent event) {
        Transaction transaction = findTransactionOrThrow(event.transactionId());

        if (isAlreadyProcessed(transaction, TRANSACTION_CLOSED_PAYMENT_FAILED, event.correlationId())) {
            return;
        }
        if (!isExpectedStatus(transaction, TRANSACTION_PAYMENT_PENDING, event.correlationId())) {
            return;
        }

        transition(transaction, TRANSACTION_CLOSED_PAYMENT_FAILED, ChangedBy.SYSTEM, null);
    }

    @Transactional
    public void closeByPaymentTimeout() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        List<Transaction> expired = transactionRepository.findByStatusInAndExpiresAtBefore(
                List.of(TRANSACTION_WAITING_FOR_PAYMENT, TRANSACTION_PAYMENT_PENDING), now);

        for (Transaction transaction : expired) {
            transition(transaction, TRANSACTION_CLOSED_PAYMENT_TIMEOUT, ChangedBy.SYSTEM, null);
        }

        if (!expired.isEmpty()) {
            log.info("Encerradas {} transações por timeout de pagamento (24h)", expired.size());
        }
    }

    @Transactional
    public void closeByDeliveryInactive() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        List<Transaction> expired = transactionRepository.findByStatusInAndExpiresAtBefore(
                List.of(DELIVERY_PENDING), now);

        for (Transaction transaction : expired) {
            transition(transaction, TRANSACTION_CLOSED_DELIVERY_INACTIVE, ChangedBy.SYSTEM, null);
        }

        if (!expired.isEmpty()) {
            log.info("Encerradas {} transações por inatividade de entrega (7d)", expired.size());
        }
    }

    @Transactional
    public void closeByGlobalTimeout() {
        LocalDateTime cutoff = LocalDateTime.now(ZoneOffset.UTC).minusDays(11);
        List<Transaction> stale = transactionRepository.findNonFinalByCreatedAtBefore(cutoff);

        for (Transaction transaction : stale) {
            transition(transaction, TRANSACTION_CLOSED_TIMEOUT, ChangedBy.SYSTEM, null);
        }

        if (!stale.isEmpty()) {
            log.info("Encerradas {} transações pelo failsafe global (11d)", stale.size());
        }
    }

    private Transaction findTransactionOrThrow(Long transactionId) {
        return transactionRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private void assertParticipant(Transaction transaction, UUID userId) {
        if (!transaction.getBuyerId().equals(userId) && !transaction.getSellerId().equals(userId)) {
            throw new UnauthorizedTransactionAccessException();
        }
    }

    private boolean isAlreadyProcessed(
            Transaction transaction,
            TransactionStatus targetStatus,
            java.util.UUID correlationId) {

        if (transaction.getStatus() == targetStatus) {
            log.warn(
                    "Evento duplicado para correlationId={}: transação {} já está em {}",
                    correlationId,
                    transaction.getId(),
                    targetStatus);
            return true;
        }
        return false;
    }

    private boolean isExpectedStatus(
            Transaction transaction,
            TransactionStatus expectedStatus,
            java.util.UUID correlationId) {

        if (transaction.getStatus() == expectedStatus) {
            return true;
        }

        log.warn(
                "Status inválido para correlationId={}: transação {} está em {}, esperado {}",
                correlationId,
                transaction.getId(),
                transaction.getStatus(),
                expectedStatus);
        return false;
    }

    private int toAmountInCents(BigDecimal winnerBidValue) {
        return winnerBidValue.movePointRight(2).intValueExact();
    }

    private LocalDateTime toLocalDateTime(Instant occurredAt) {
        if (occurredAt == null) {
            return LocalDateTime.now(ZoneOffset.UTC);
        }
        return LocalDateTime.ofInstant(occurredAt, ZoneOffset.UTC);
    }

    private Instant toInstant(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    private Instant toInstant(Instant occurredAt) {
        return occurredAt != null ? occurredAt : Instant.now();
    }

    private boolean isClosedStatus(TransactionStatus status) {
        return switch (status) {
            case TRANSACTION_CLOSED_DELIVERY_INACTIVE,
                 TRANSACTION_CLOSED_PAYMENT_FAILED,
                 TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED,
                 TRANSACTION_CLOSED_PAYMENT_TIMEOUT,
                 TRANSACTION_CLOSED_TIMEOUT -> true;
            default -> false;
        };
    }

    private void publishStatusEvent(Transaction transaction, TransactionStatus status, Instant occurredAt) {
        eventProducer.publishStatusEvent(new TransactionStatusEvent(
                transaction.getCorrelationId(),
                transaction.getId(),
                transaction.getAuctionId(),
                transaction.getSellerId(),
                transaction.getBuyerId(),
                status,
                occurredAt));
    }

    private void publishClosedEvent(Transaction transaction, TransactionStatus status, Instant occurredAt) {
        TransactionClosedReason closedReason = TransactionClosedReason.fromStatus(status);
        eventProducer.publishClosedEvent(new TransactionClosedEvent(
                transaction.getCorrelationId(),
                transaction.getId(),
                transaction.getAuctionId(),
                transaction.getSellerId(),
                transaction.getBuyerId(),
                status,
                closedReason.message(),
                occurredAt,
                closedReason.penalty()));
    }

    private void publishPaymentRequested(Transaction transaction) {
        eventProducer.publishPaymentRequested(new PaymentRequestedEvent(
                transaction.getCorrelationId(),
                transaction.getAuctionId(),
                transaction.getId(),
                transaction.getBuyerId(),
                transaction.getAmountInCents(),
                PaymentRequestedEvent.DEFAULT_EXPIRES_IN_SECONDS));
    }
}
