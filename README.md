# transaction-service

Microserviço de **transações pós-leilão** do projeto **Leiloeiro Online**.

Orquestra o ciclo pós-leilão: cria transação, solicita pagamento, reage a eventos Kafka, expõe consulta de status e confirmação de entrega, e executa timeouts (24h, 7d, 11d).

**Stack:** Java 25 · Spring Boot 4.0.6 · Spring Cloud 2025.1.1 · PostgreSQL 17 · Kafka 4.1.0 · Docker Compose

---

## Como inicializar o projeto

```bash
cd transaction-service
docker compose up -d --build
```

| Serviço | Porta | URL / uso |
|---------|-------|-----------|
| `transaction-service` | **9082** | API (`profile prod`) |
| `kafka-ui` | **8085** | http://localhost:8085 |
| `kafka` | 9092 | Broker (`apache/kafka:4.1.0`) |
| `postgres` | 5432 | Schema `transaction_service` |

O schema SQL é aplicado automaticamente via `./db/init/01_transaction_schema.sql` na primeira subida.

### Verificar se subiu

```bash
docker compose ps
curl http://localhost:9082/actuator/health
# {"status":"UP","groups":["liveness","readiness"]}
curl http://localhost:9082/actuator/prometheus   # métricas (profile prod)
```

### Parar tudo

```bash
docker compose down
```

> Eureka, Gateway e Keycloak **não** são necessários para rodar localmente (Eureka desligado no compose).

---

## Testar manualmente (sem outros microserviços)

1. `docker compose up -d --build`
2. Abrir **Kafka UI:** http://localhost:8085 (cluster `transaction-local`)
3. Publicar JSON no tópico `auctions.lot.ended-with-winner`
4. Simular eventos em `payments.payment.*` conforme o fluxo
5. Verificar banco:
   ```bash
   docker exec transaction-postgres psql -U postgres -d postgres \
     -c "SELECT id, status FROM transaction_service.transactions;"
   ```
6. Consultar status (comprador ou vendedor — o front pode chamar em polling):
   ```bash
   curl http://localhost:9082/transactions/1 \
     -H "X-User-Id: bbbbbbbb-cccc-dddd-eeee-222222222222"
   # → {"id":1,"status":"DELIVERY_PENDING"}
   ```
7. Confirmar entrega (somente comprador, com transação em `DELIVERY_PENDING`):
   ```bash
   curl -X POST http://localhost:9082/transactions/1/confirm-delivery \
     -H "X-User-Id: bbbbbbbb-cccc-dddd-eeee-222222222222"
   ```

---

## Testes automatizados

Via Docker (`eclipse-temurin:25-jdk-jammy` + Maven):

```bash
# Suíte completa (61 testes)
docker run --rm -v "$(pwd)":/app -w /app eclipse-temurin:25-jdk-jammy \
  bash -c "apt-get update -qq && apt-get install -y -qq maven && mvn -q test"

# Só E2E de fluxo completo
docker run --rm -v "$(pwd)":/app -w /app eclipse-temurin:25-jdk-jammy \
  bash -c "apt-get update -qq && apt-get install -y -qq maven && mvn -q test -Dtest=TransactionFlowIntegrationTest"
```

| Classe | Cobertura |
|--------|-----------|
| `TransactionStateMachineTest` | Transições, `expires_at`, estados finais |
| `TransactionControllerTest` | GET status + confirm-delivery: 401, 403, 404, 409, 200, 204 |
| `TransactionTimeoutWorkerTest` | Timeouts 24h, 7d, 11d |
| `AuctionEndedWithWinnerConsumerTest` | Consumer + idempotência |
| `TransactionFlowIntegrationTest` | E2E: happy path, falhas, timeout 7d |
| `TransactionClosedReasonTest` | Mapeamento status → reason → penalty |

Profile de teste: `application-test.yaml` (H2 + EmbeddedKafka).

---

## Fluxo resumido

| Etapa | Entrada | Saída / status |
|-------|---------|----------------|
| 1 | `AuctionEndedWithWinner` | `TransactionCreated` → `PaymentRequested` → `TransactionWaitingForPayment` |
| 1b | `PaymentCreated` / `PaymentCreatedFailed` | `TransactionPaymentPending` ou `TransactionClosedPaymentCreatedFailed` (sem punição) |
| 2 | `PaymentReceived` / `PaymentExpired` | `TransactionDeliveryPending` ou `TransactionClosedPaymentFailed` (**com punição**) |
| 3 | `POST /transactions/{id}/confirm-delivery` | `TransactionFinished` |
| 4 | Timeout 7d em `DELIVERY_PENDING` | `TransactionClosedDeliveryInactive` |

**Workers:** 24h sem resposta do payment → `TransactionClosedPaymentTimeOut` (sem punição) · 11d failsafe global → `TransactionClosedTimeout`

**Regras-chave:** 1 tentativa de pagamento · punição **somente** em `TransactionClosedPaymentFailed` · disputas são manuais e não pausam o cronômetro

---

## API REST

### Consultar status

```http
GET /transactions/{id}
X-User-Id: {uuid-do-comprador-ou-vendedor}
```

Retorna o **status atual** da transação (`id` + `status`). O front usa esse endpoint para acompanhar o ciclo pós-leilão em tempo quase real (polling): aguardando pagamento, pagamento pendente, entrega, finalizado ou encerrado.

| Código | Situação |
|--------|----------|
| 200 | `{"id": 1, "status": "DELIVERY_PENDING"}` — qualquer status válido do domínio |
| 401 | `X-User-Id` ausente |
| 403 | Usuário não é comprador nem vendedor da transação |
| 404 | Transação não encontrada |

Comprador **e** vendedor podem consultar. Não altera estado — apenas leitura do que já está no banco (atualizado via Kafka e workers).

### Confirmar entrega

```http
POST /transactions/{id}/confirm-delivery
X-User-Id: {uuid-do-comprador}
```

| Código | Situação |
|--------|----------|
| 204 | `DELIVERY_PENDING` → `TRANSACTION_FINISHED` |
| 401 | `X-User-Id` ausente |
| 403 | Usuário não é o comprador |
| 404 | Transação não encontrada |
| 409 | Status inválido |

Em produção, JWT é validado no API Gateway; o serviço recebe apenas `X-User-Id`.

---

## Kafka

### Consumidos (5)

| Evento | Tópico |
|--------|--------|
| `AuctionEndedWithWinner` | `auctions.lot.ended-with-winner` |
| `PaymentCreated` | `payments.payment.created` |
| `PaymentCreatedFailed` | `payments.payment.created-failed` |
| `PaymentReceived` | `payments.payment.received` |
| `PaymentExpired` | `payments.payment.expired` |

### Produzidos (7)

| Evento | Tópico |
|--------|--------|
| `PaymentRequested` | `transactions.payment.requested` |
| `TransactionCreated` | `transactions.status.created` |
| `TransactionWaitingForPayment` | `transactions.status.waiting-for-payment` |
| `TransactionPaymentPending` | `transactions.status.payment-pending` |
| `TransactionDeliveryPending` | `transactions.status.delivery-pending` |
| `TransactionFinished` | `transactions.status.finished` |
| `TransactionClosed` | `transactions.status.closed` |

Os 5 eventos de status não-closed compartilham schema `TransactionStatusEvent` (`correlationId`, `transactionId`, `auctionId`, `sellerId`, `highestBidderId`, `status`, `occurredAt`).

Os 5 encerramentos (`TRANSACTION_CLOSED_*`) publicam no tópico único `transactions.status.closed` como `TransactionClosedEvent`, com os mesmos campos acima mais `reason` (mensagem em PT) e `penalty` (`true` **somente** para `TRANSACTION_CLOSED_PAYMENT_FAILED`).

---

## Estados

| Status | Final? |
|--------|--------|
| `TRANSACTION_CREATED` | Não |
| `TRANSACTION_WAITING_FOR_PAYMENT` | Não |
| `TRANSACTION_PAYMENT_PENDING` | Não |
| `DELIVERY_PENDING` | Não |
| `TRANSACTION_FINISHED` | **Sim** |
| `TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED` | **Sim** (sem punição) |
| `TRANSACTION_CLOSED_PAYMENT_FAILED` | **Sim** (com punição) |
| `TRANSACTION_CLOSED_PAYMENT_TIMEOUT` | **Sim** (sem punição) |
| `TRANSACTION_CLOSED_DELIVERY_INACTIVE` | **Sim** |
| `TRANSACTION_CLOSED_TIMEOUT` | **Sim** |

---

## Estrutura

```
src/main/java/br/com/infnet/transactionService/
├── config/       # Kafka, ObjectMapper, Scheduling
├── controller/   # REST (GET status, confirm-delivery)
├── domain/       # Transaction, TransactionHistory
├── kafka/        # 5 consumers + producer
├── metrics/    # Métricas Micrometer (consumers, transições, REST)
├── service/      # TransactionService, StateMachine, History
└── worker/       # TransactionTimeoutWorker
src/test/java/.../integration/   # Testes E2E
db/init/          # Schema PostgreSQL
```

---

## Observabilidade

Logs com `correlationId` (MDC) nos consumers Kafka · métricas Micrometer · Actuator com `health`, `info`, `prometheus` e `metrics` em prod (`/actuator/prometheus`).
