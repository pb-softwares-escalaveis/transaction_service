package br.com.infnet.transactionService.exception;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(Long transactionId) {
        super("Transação não encontrada: id=%d".formatted(transactionId));
    }
}
