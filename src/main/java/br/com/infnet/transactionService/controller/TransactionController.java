package br.com.infnet.transactionService.controller;

import br.com.infnet.transactionService.exception.MissingUserIdHeaderException;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
public class TransactionController {

    public static final String USER_ID_HEADER = "X-User-Id";

    private final TransactionService transactionService;

    @PostMapping("/{id}/confirm-delivery")
    public ResponseEntity<Void> confirmDelivery(
            @PathVariable Long id,
            @RequestHeader(value = USER_ID_HEADER, required = false) String userIdHeader) {

        if (userIdHeader == null || userIdHeader.isBlank()) {
            throw new MissingUserIdHeaderException();
        }

        UUID userId;
        try {
            userId = UUID.fromString(userIdHeader.trim());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Header X-User-Id deve ser um UUID válido");
        }

        transactionService.confirmDelivery(id, userId);
        return ResponseEntity.noContent().build();
    }
}
