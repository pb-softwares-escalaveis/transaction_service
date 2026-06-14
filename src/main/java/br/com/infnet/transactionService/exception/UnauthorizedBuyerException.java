package br.com.infnet.transactionService.exception;

public class UnauthorizedBuyerException extends RuntimeException {

    public UnauthorizedBuyerException() {
        super("Usuário não autorizado a confirmar entrega desta transação");
    }
}
