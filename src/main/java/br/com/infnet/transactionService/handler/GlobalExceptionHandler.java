package br.com.infnet.transactionService.handler;

import br.com.infnet.transactionService.dto.ErrorResponse;
import br.com.infnet.transactionService.exception.InvalidStateTransitionException;
import br.com.infnet.transactionService.exception.MissingUserIdHeaderException;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.exception.UnauthorizedBuyerException;
import br.com.infnet.transactionService.exception.UnauthorizedTransactionAccessException;
import br.com.infnet.transactionService.metrics.TransactionMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.HandlerMapping;

@RestControllerAdvice
@RequiredArgsConstructor
@Slf4j
public class GlobalExceptionHandler {

    private final TransactionMetrics transactionMetrics;

    @ExceptionHandler(MissingUserIdHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingUserIdHeader(
            MissingUserIdHeaderException ex, WebRequest request) {
        recordRestError(request, "401");
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(new ErrorResponse("MISSING_USER_ID", ex.getMessage()));
    }

    @ExceptionHandler(UnauthorizedBuyerException.class)
    public ResponseEntity<ErrorResponse> handleUnauthorizedBuyer(UnauthorizedBuyerException ex, WebRequest request) {
        recordRestError(request, "403");
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(new ErrorResponse("UNAUTHORIZED_BUYER", ex.getMessage()));
    }

    @ExceptionHandler(UnauthorizedTransactionAccessException.class)
    public ResponseEntity<ErrorResponse> handleUnauthorizedTransactionAccess(
            UnauthorizedTransactionAccessException ex, WebRequest request) {
        recordRestError(request, "403");
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(new ErrorResponse("UNAUTHORIZED_TRANSACTION_ACCESS", ex.getMessage()));
    }

    @ExceptionHandler(TransactionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleTransactionNotFound(
            TransactionNotFoundException ex, WebRequest request) {
        recordRestError(request, "404");
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("TRANSACTION_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    public ResponseEntity<ErrorResponse> handleInvalidStateTransition(
            InvalidStateTransitionException ex, WebRequest request) {
        recordRestError(request, "409");
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("INVALID_STATE_TRANSITION", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, WebRequest request) {
        recordRestError(request, "400");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse("BAD_REQUEST", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, WebRequest request) {
        recordRestError(request, "500");
        log.error("Erro interno na API", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("INTERNAL_ERROR", "Ocorreu um erro interno. Tente novamente mais tarde."));
    }

    private void recordRestError(WebRequest request, String statusCode) {
        transactionMetrics.incrementRestRequest(resolveEndpoint(request), statusCode);
    }

    @SuppressWarnings("unused")
    private String resolveEndpoint(WebRequest request) {
        if (request instanceof ServletWebRequest servletWebRequest) {
            String method = servletWebRequest.getRequest().getMethod();
            Object pattern = servletWebRequest.getRequest()
                    .getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            if (pattern instanceof String pathPattern) {
                return method + " " + pathPattern;
            }
        }
        return "UNKNOWN";
    }
}
