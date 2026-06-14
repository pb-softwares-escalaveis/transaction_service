package br.com.infnet.transactionService.controller;

import br.com.infnet.transactionService.exception.InvalidStateTransitionException;
import br.com.infnet.transactionService.exception.TransactionNotFoundException;
import br.com.infnet.transactionService.exception.UnauthorizedBuyerException;
import br.com.infnet.transactionService.handler.GlobalExceptionHandler;
import br.com.infnet.transactionService.service.TransactionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_PAYMENT_PENDING;
import static br.com.infnet.transactionService.enums.TransactionStatus.TRANSACTION_FINISHED;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TransactionController.class)
@Import(GlobalExceptionHandler.class)
class TransactionControllerTest {

    private static final Long TRANSACTION_ID = 42L;
    private static final UUID BUYER_ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private static final UUID OTHER_USER_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TransactionService transactionService;

    @Test
    void shouldReturn401WhenUserIdHeaderIsMissing() throws Exception {
        mockMvc.perform(post("/transactions/{id}/confirm-delivery", TRANSACTION_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.codigo").value("MISSING_USER_ID"))
                .andExpect(jsonPath("$.mensagem").value("Header X-User-Id é obrigatório"));
    }

    @Test
    void shouldReturn403WhenUserIsNotTheBuyer() throws Exception {
        doThrow(new UnauthorizedBuyerException())
                .when(transactionService)
                .confirmDelivery(TRANSACTION_ID, OTHER_USER_ID);

        mockMvc.perform(post("/transactions/{id}/confirm-delivery", TRANSACTION_ID)
                        .header(TransactionController.USER_ID_HEADER, OTHER_USER_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.codigo").value("UNAUTHORIZED_BUYER"))
                .andExpect(jsonPath("$.mensagem")
                        .value("Usuário não autorizado a confirmar entrega desta transação"));
    }

    @Test
    void shouldReturn404WhenTransactionDoesNotExist() throws Exception {
        doThrow(new TransactionNotFoundException(TRANSACTION_ID))
                .when(transactionService)
                .confirmDelivery(TRANSACTION_ID, BUYER_ID);

        mockMvc.perform(post("/transactions/{id}/confirm-delivery", TRANSACTION_ID)
                        .header(TransactionController.USER_ID_HEADER, BUYER_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.codigo").value("TRANSACTION_NOT_FOUND"))
                .andExpect(jsonPath("$.mensagem").value("Transação não encontrada: id=42"));
    }

    @Test
    void shouldReturn409WhenTransactionIsNotInDeliveryPending() throws Exception {
        doThrow(new InvalidStateTransitionException(TRANSACTION_PAYMENT_PENDING, TRANSACTION_FINISHED))
                .when(transactionService)
                .confirmDelivery(TRANSACTION_ID, BUYER_ID);

        mockMvc.perform(post("/transactions/{id}/confirm-delivery", TRANSACTION_ID)
                        .header(TransactionController.USER_ID_HEADER, BUYER_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.codigo").value("INVALID_STATE_TRANSITION"))
                .andExpect(jsonPath("$.mensagem")
                        .value("Transição inválida de TRANSACTION_PAYMENT_PENDING para TRANSACTION_FINISHED"));
    }

    @Test
    void shouldReturn204WhenDeliveryIsConfirmedSuccessfully() throws Exception {
        doNothing().when(transactionService).confirmDelivery(TRANSACTION_ID, BUYER_ID);

        mockMvc.perform(post("/transactions/{id}/confirm-delivery", TRANSACTION_ID)
                        .header(TransactionController.USER_ID_HEADER, BUYER_ID)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent());

        verify(transactionService).confirmDelivery(TRANSACTION_ID, BUYER_ID);
    }
}
