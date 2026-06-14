package br.com.infnet.transactionService.exception;

import br.com.infnet.transactionService.enums.TransactionStatus;

public class InvalidStateTransitionException extends RuntimeException {

    public InvalidStateTransitionException(TransactionStatus from, TransactionStatus to) {
        super("Transição inválida de %s para %s".formatted(from, to));
    }
}
