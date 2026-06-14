package br.com.infnet.transactionService.exception;

public class MissingUserIdHeaderException extends RuntimeException {

    public MissingUserIdHeaderException() {
        super("Header X-User-Id é obrigatório");
    }
}
