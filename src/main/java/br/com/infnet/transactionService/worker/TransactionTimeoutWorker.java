package br.com.infnet.transactionService.worker;

import br.com.infnet.transactionService.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionTimeoutWorker {

    private final TransactionService transactionService;

    @Scheduled(fixedDelayString = "${transaction.worker.delay-ms:60000}")
    @Transactional
    public void processTimeouts() {
        log.debug("Iniciando processamento de timeouts de transação");
        transactionService.closeByPaymentTimeout();
        transactionService.closeByDeliveryInactive();
        transactionService.closeByGlobalTimeout();
        log.debug("Processamento de timeouts concluído");
    }
}
