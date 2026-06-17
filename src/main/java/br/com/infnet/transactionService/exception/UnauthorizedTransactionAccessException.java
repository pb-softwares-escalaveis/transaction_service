package br.com.infnet.transactionService.exception;

public class UnauthorizedTransactionAccessException extends RuntimeException {

    public UnauthorizedTransactionAccessException() {
        super("Usuário não autorizado a consultar esta transação");
    }
}
