package br.com.infnet.transactionService.controller;

import br.com.infnet.transactionService.dto.TransactionStatusResponse;
import br.com.infnet.transactionService.exception.MissingUserIdHeaderException;
import br.com.infnet.transactionService.metrics.TransactionMetrics;
import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    public static final String USER_ID_HEADER = "X-User-Id";
    private static final String GET_STATUS_ENDPOINT = "GET /transactions/{id}";
    private static final String CONFIRM_DELIVERY_ENDPOINT = "POST /transactions/{id}/confirm-delivery";

    private final TransactionService transactionService;
    private final TransactionMetrics transactionMetrics;

    @GetMapping("/{id}")
    public ResponseEntity<TransactionStatusResponse> getStatus(
            @PathVariable Long id,
            @RequestHeader(value = USER_ID_HEADER, required = false) String userIdHeader) {

        log.info("Consulta de status: transactionId={}", id);
        TransactionStatusResponse response = transactionService.getStatus(id, parseUserId(userIdHeader));
        transactionMetrics.incrementRestRequest(GET_STATUS_ENDPOINT, "200");
        log.info("Status retornado: transactionId={}, status={}", id, response.status());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{id}/confirm-delivery")
    public ResponseEntity<Void> confirmDelivery(
            @PathVariable Long id,
            @RequestHeader(value = USER_ID_HEADER, required = false) String userIdHeader) {

        log.info("Confirmação de entrega solicitada: transactionId={}", id);
        transactionService.confirmDelivery(id, parseUserId(userIdHeader));
        transactionMetrics.incrementRestRequest(CONFIRM_DELIVERY_ENDPOINT, "204");
        log.info("Entrega confirmada: transactionId={}", id);
        return ResponseEntity.noContent().build();
    }

    private UUID parseUserId(String userIdHeader) {
        if (userIdHeader == null || userIdHeader.isBlank()) {
            throw new MissingUserIdHeaderException();
        }

        try {
            return UUID.fromString(userIdHeader.trim());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Header X-User-Id deve ser um UUID válido");
        }
    }
}
