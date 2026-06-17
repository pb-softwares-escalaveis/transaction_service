package br.com.infnet.transactionService.dto;

import br.com.infnet.transactionService.enums.TransactionStatus;

public record TransactionStatusResponse(Long id, TransactionStatus status) {
}
