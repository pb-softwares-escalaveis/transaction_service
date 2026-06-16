package br.com.infnet.transactionService.enums;

import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_DELIVERY_INACTIVE;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_FAILED;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_PAYMENT_TIMEOUT;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_CLOSED_TIMEOUT;

public enum TransactionClosedReason {

    DELIVERY_INACTIVE(
            TRANSACTION_CLOSED_DELIVERY_INACTIVE,
            "A etapa de entrega permaneceu sem movimentação durante o período permitido.",
            false),
    PAYMENT_FAILED(
            TRANSACTION_CLOSED_PAYMENT_FAILED,
            "O pagamento não pôde ser concluído ou expirou antes da confirmação.",
            true),
    PAYMENT_CREATED_FAILED(
            TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED,
            "Ocorreu um erro técnico durante a geração da cobrança. Nenhuma penalidade foi aplicada.",
            false),
    PAYMENT_TIMEOUT(
            TRANSACTION_CLOSED_PAYMENT_TIMEOUT,
            "Não foi possível obter resposta do serviço de pagamento dentro do prazo esperado. Nenhuma penalidade foi aplicada.",
            false),
    TIMEOUT(
            TRANSACTION_CLOSED_TIMEOUT,
            "O prazo máximo da negociação foi atingido sem a conclusão do pagamento.",
            false);

    private final TransactionStatus status;
    private final String message;
    private final boolean penalty;

    TransactionClosedReason(TransactionStatus status, String message, boolean penalty) {
        this.status = status;
        this.message = message;
        this.penalty = penalty;
    }

    public TransactionStatus status() {
        return status;
    }

    public String message() {
        return message;
    }

    public boolean penalty() {
        return penalty;
    }

    public static TransactionClosedReason fromStatus(TransactionStatus status) {
        for (TransactionClosedReason reason : values()) {
            if (reason.status == status) {
                return reason;
            }
        }
        throw new IllegalArgumentException("Status não é um fechamento de transação: " + status);
    }
}
