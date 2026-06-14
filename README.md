# transaction-service

Microserviço de **transações pós-leilão** do projeto **Leiloeiro Online**.

## Visão geral

O microserviço de Transação orquestra o **ciclo de vida pós-leilão**. Consolida a gestão de inatividade (timeout), o modelo de intervenção manual para casos de denúncias e disputas, e camadas de resiliência sistêmica para evitar transações pendentes indefinidamente.

Responsabilidades:

- Criar transação quando um leilão termina com vencedor
- Solicitar cobrança ao payment-service
- Reagir a eventos de pagamento (sucesso, falha, expiração)
- Expor endpoint REST para confirmação de entrega pelo comprador
- Executar workers de timeout (24h, 7d, 11d)
- Publicar eventos de status para notificação e serviços downstream

---

## Stack e infraestrutura

| Componente | Versão / detalhe |
|------------|------------------|
| Java | 25 |
| Spring Boot | 4.0.6 |
| Spring Cloud | 2025.1.1 |
| PostgreSQL | 17 — schema exclusivo `transaction_service` |
| Apache Kafka | 4.1.0 |
| Discovery | Eureka Client (`transaction-service`) |
| Gateway | API Gateway + Keycloak (JWT validado no gateway) |
| Observabilidade | Spring Boot Actuator (`health`, `info`) |
| Containerização | Docker + Docker Compose |

Portas:

| Ambiente | Porta |
|----------|-------|
| Dev (host) | **9082** |
| Prod (container) | **8080** (mapeada para 9082 no compose) |

---

## Setup rápido

### 1. Subir infraestrutura e aplicação

```bash
docker compose up -d --build
```

| Serviço | Porta (host) | Descrição |
|---------|--------------|-----------|
| `postgres` | 5432 | Banco com schema `transaction_service` |
| `kafka` | 9092 | Broker Kafka |
| `transaction-service` | **9082** | API (profile `prod`) |

O schema SQL é aplicado via `./db/init/01_transaction_schema.sql` na primeira subida do Postgres.

### 2. Verificar saúde

```bash
curl http://localhost:9082/actuator/health
# {"status":"UP"}
```

### 3. Desenvolvimento local (profile `dev`)

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

> **Nota:** Se o host tiver apenas Java 21, use Docker (`docker compose up`) ou rode testes via imagem Java 25 (seção Testes).

---

## Fluxo de produção (ordem cronológica)

### ETAPA 1 — Início e cobrança

**1. Entrada (Kafka):** `AuctionEndedWithWinner`

- Criação da transação
- **Saída:** `TransactionCreated` → status `TRANSACTION_CREATED`

**2. Solicitação de pagamento**

- **Saída:** `PaymentRequested` (para o payment-service)
- **Saída:** `TransactionWaitingForPayment` → status `TRANSACTION_WAITING_FOR_PAYMENT`

**3. Resposta da criação do pagamento**

Entrada: `PaymentCreated` (sucesso) ou `PaymentCreatedFailed` (falha).

| Cenário | Saída Kafka | Status |
|---------|-------------|--------|
| Sucesso | `TransactionPaymentPending` | `TRANSACTION_PAYMENT_PENDING` |
| Falha técnica | `TransactionClosedPaymentCreatedFailed` | `TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED` |

Em falha técnica imediata, o vendedor é notificado para decidir sobre reabertura do lote. **Não há punição** neste cenário.

### ETAPA 2 — Processamento do pagamento

O sistema aguarda um dos eventos do payment-service:

- `PaymentReceived` (sucesso)
- `PaymentExpired` (falha por expiração)

### ETAPA 3 — Ramificação de pagamento

| Cenário | Saída Kafka | Status |
|---------|-------------|--------|
| `PaymentReceived` | `TransactionDeliveryPending` | `DELIVERY_PENDING` |
| `PaymentExpired` | `TransactionClosedPaymentFailed` | `TRANSACTION_CLOSED_PAYMENT_FAILED` |

No cenário de falha, a transação é encerrada definitivamente. O **notification-service** é avisado e o **user-service** processa banimento/suspensão do comprador.

### ETAPA 4 — Gestão de entrega

**Aguardando confirmação:** o sistema permanece em `DELIVERY_PENDING` por **7 dias** até o comprador confirmar via API. Se não confirmar, segue para a ETAPA 5.

**Finalização com sucesso:** `POST /transactions/{id}/confirm-delivery`

- **Saída:** `TransactionFinished` → status `TRANSACTION_FINISHED`
- Processo encerrado. Anúncio/lote fechado.

### ETAPA 5 — Encerramento por inatividade ou disputa

**5.1 Timeout final (7 dias):** sem confirmação nem denúncia → `TRANSACTION_CLOSED_DELIVERY_INACTIVE`

- **Saída:** `TransactionClosedDeliveryInactive`
- Processo encerrado. Anúncio/lote fechado.

**5.2 Disputa (manual):** denúncias via report-service são tratadas **manualmente** por administradores. A denúncia **não interrompe** o cronômetro automático — o timeout de inatividade ocorre normalmente se o prazo expirar.

---

## Comunicação Kafka

### Eventos consumidos (5)

| Evento | Tópico | Payload | Descrição |
|--------|--------|---------|-----------|
| `AuctionEndedWithWinner` | `auctions.lot.ended-with-winner` | `correlationId`, `auctionId`, `sellerId`, `highestBidderId`, `winnerBidValue`, `occurredAt` | Gatilho inicial com dados do vencedor |
| `PaymentCreated` | `payments.payment.created` | `correlationId`, `transactionId`, `paymentId`, `amountInCents`, `status` | Cobrança criada com sucesso |
| `PaymentCreatedFailed` | `payments.payment.created.failed` | `correlationId`, `transactionId`, `reason` | Falha técnica ao gerar cobrança |
| `PaymentReceived` | `payments.payment.received` | `correlationId`, `bidderId`, `paymentId`, `transactionId`, `auctionId`, `occurredAt` | Pagamento recebido |
| `PaymentExpired` | `payments.payment.expired` | `correlationId`, `bidderId`, `paymentId`, `transactionId`, `auctionId`, `occurredAt` | Prazo de pagamento expirou |

### Eventos produzidos (11)

| Evento | Tópico | Payload (campos principais) |
|--------|--------|----------------------------|
| `PaymentRequested` | `transactions.payment.requested` | `correlationId`, `auctionId`, `transactionId`, `highestBidderId`, `amountInCents`, `expiresInSeconds` (86400) |
| `TransactionCreated` | `transactions.status.created` | `correlationId`, `transactionId`, `auctionId`, `sellerId`, `highestBidderId`, `status`, `occurredAt` |
| `TransactionWaitingForPayment` | `transactions.status.waiting-for-payment` | idem |
| `TransactionPaymentPending` | `transactions.status.payment-pending` | idem |
| `TransactionClosedPaymentCreatedFailed` | `transactions.status.closed.payment-created-failed` | idem |
| `TransactionDeliveryPending` | `transactions.status.delivery-pending` | idem |
| `TransactionFinished` | `transactions.status.finished` | idem |
| `TransactionClosedPaymentFailed` | `transactions.status.closed.payment-failed` | idem — **dispara punição** |
| `TransactionClosedPaymentTimeOut` | `transactions.status.closed.payment-timeout` | idem |
| `TransactionClosedDeliveryInactive` | `transactions.status.closed.delivery-inactive` | idem |
| `TransactionClosedTimeout` | `transactions.status.closed.timeout` | idem |

Os 10 eventos de status compartilham o mesmo schema. O campo `status` no JSON reflete o estado atual da transação. O campo `highestBidderId` representa o comprador vencedor (`buyer_id` internamente).

---

## API REST

### `POST /transactions/{id}/confirm-delivery`

Acionado pelo **comprador** para confirmar o recebimento e encerrar o ciclo com sucesso.

```http
POST /transactions/{id}/confirm-delivery
X-User-Id: {uuid-do-comprador}
```

| Código | Situação |
|--------|----------|
| **204** | Entrega confirmada — `DELIVERY_PENDING` → `TRANSACTION_FINISHED` |
| **401** | Header `X-User-Id` ausente |
| **403** | Usuário não é o comprador |
| **404** | Transação não encontrada |
| **409** | Status diferente de `DELIVERY_PENDING` |

```bash
curl -X POST http://localhost:9082/transactions/1/confirm-delivery \
  -H "X-User-Id: bbbbbbbb-cccc-dddd-eeee-222222222222"
```

Em produção, JWT é validado pelo API Gateway + Keycloak; o serviço recebe apenas `X-User-Id`.

---

## Estados da transação

| Status | Descrição | Final? |
|--------|-----------|--------|
| `TRANSACTION_CREATED` | Transação criada | Não |
| `TRANSACTION_WAITING_FOR_PAYMENT` | Aguardando criação da cobrança | Não |
| `TRANSACTION_PAYMENT_PENDING` | Cobrança criada, aguardando pagamento | Não |
| `TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED` | Falha técnica na cobrança (**sem punição**) | **Sim** |
| `DELIVERY_PENDING` | Pagamento confirmado, aguardando entrega | Não |
| `TRANSACTION_FINISHED` | Ciclo concluído com sucesso | **Sim** |
| `TRANSACTION_CLOSED_PAYMENT_FAILED` | Pagamento recusado/expirado (**com punição**) | **Sim** |
| `TRANSACTION_CLOSED_PAYMENT_TIMEOUT` | Sem resposta do payment-service em 24h (**sem punição**) | **Sim** |
| `TRANSACTION_CLOSED_DELIVERY_INACTIVE` | Sem confirmação de entrega em 7d | **Sim** |
| `TRANSACTION_CLOSED_TIMEOUT` | Failsafe global 11d | **Sim** |

Estados finais bloqueiam novas transições.

---

## Regras de negócio

**Uma transação por comprador** — cada tentativa de compra gera uma transação independente. Falha no pagamento encerra definitivamente (1 tentativa).

**Intervenção manual (disputas)** — denúncias de entrega são tratadas manualmente pela administração. O fluxo de timeout permanece ativo; a decisão humana é soberana.

**Punição automática** — disparada **somente** por `TransactionClosedPaymentFailed` (`transactions.status.closed.payment-failed`). Payload com `sellerId` + `highestBidderId`. Consumido pelo user-service.

**Sem punição:**

- `TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED` — falha técnica na cobrança
- `TRANSACTION_CLOSED_PAYMENT_TIMEOUT` — worker 24h

**Inércia do comprador** — sem confirmar entrega, o sistema encerra por timeout de inatividade (7d).

---

## Worker de timeouts

`TransactionTimeoutWorker` executa periodicamente (padrão: 60s, configurável via `transaction.worker.delay-ms`):

### A. Timeout de pagamento (24 horas)

Transação em `TRANSACTION_WAITING_FOR_PAYMENT` ou `TRANSACTION_PAYMENT_PENDING` por mais de 24h sem resposta do payment-service:

- **Ação:** `TRANSACTION_CLOSED_PAYMENT_TIMEOUT`
- **Saída:** `TransactionClosedPaymentTimeOut`
- Se `PaymentCreatedFailed` chegar antes, encerramento imediato com `TRANSACTION_CLOSED_PAYMENT_CREATED_FAILED`

### B. Timeout de entrega (7 dias)

Transação em `DELIVERY_PENDING` por 7 dias sem confirmação:

- **Ação:** `TRANSACTION_CLOSED_DELIVERY_INACTIVE`
- **Saída:** `TransactionClosedDeliveryInactive`

### C. Timer global de segurança (11 dias)

Qualquer transação não-final após 11 dias de `created_at`:

- **Ação:** `TRANSACTION_CLOSED_TIMEOUT`
- **Saída:** `TransactionClosedTimeout`

### Atualização de `expires_at`

| Status destino | `expires_at` |
|----------------|--------------|
| `TRANSACTION_WAITING_FOR_PAYMENT` / `TRANSACTION_PAYMENT_PENDING` | `now + 24h` |
| `DELIVERY_PENDING` | `now + 7d` |
| Estados finais | Mantém último valor |

---

## Arquitetura de dados

### Tabela `transactions` (estado atual)

| Campo | Descrição |
|-------|-----------|
| `id` | PK |
| `correlation_id` | UNIQUE — idempotência |
| `auction_id` | ID do leilão |
| `buyer_id` | Comprador (`highestBidderId` do leilão) |
| `seller_id` | Vendedor |
| `payment_id` | UUID do pagamento (preenchido em `PaymentCreated`) |
| `status` | Enum de status |
| `created_at` | Base para timer global (11d) |
| `updated_at` | Última atualização |
| `expires_at` | Base para timeouts de negócio (24h / 7d) |

### Tabela `transaction_history` (auditoria)

| Campo | Descrição |
|-------|-----------|
| `id` | PK |
| `transaction_id` | FK → `transactions` |
| `old_status` | Status anterior |
| `new_status` | Status novo |
| `changed_by` | `SYSTEM`, `ADMIN` ou `USER` |
| `reason` | Motivo opcional |
| `occurred_at` | Timestamp da mudança |

---

## Testar sem outros microserviços

1. `docker compose up -d`
2. Publicar JSON no tópico `auctions.lot.ended-with-winner`
3. Verificar Postgres: `docker exec transaction-postgres psql -U postgres -d postgres -c "SELECT * FROM transaction_service.transactions;"`
4. Simular eventos nos tópicos `payments.payment.*`
5. Confirmar entrega via `curl` com `X-User-Id`

Não é necessário Eureka, Gateway ou Keycloak para testes locais básicos.

---

## Testes

```bash
./mvnw test
```

Com Java 25 via Docker (recomendado se o host tiver Java 21):

```bash
docker run --rm -v "$(pwd)":/app -w /app eclipse-temurin:25-jdk-jammy \
  bash -c "chmod +x mvnw && ./mvnw -q test"
```

Suíte (**48 testes**):

| Classe | Cobertura |
|--------|-----------|
| `TransactionStateMachineTest` | Transições válidas/inválidas, `expires_at`, estados finais |
| `TransactionControllerTest` | 401, 403, 404, 409, 204 |
| `TransactionTimeoutWorkerTest` | Regras 24h, 7d, 11d |
| `AuctionEndedWithWinnerConsumerTest` | Consumer Kafka + idempotência por `correlationId` |
| `TransactionFlowIntegrationTest` | Jornada E2E: ETAPAS 1→5 (happy path, falhas, timeout 7d) |

Profile de teste: `application-test.yaml` — H2, EmbeddedKafka, scheduling desabilitado.

---

## Estrutura do projeto

```
src/main/java/br/com/infnet/transactionService/
├── config/          # Kafka, ObjectMapper, Scheduling
├── controller/      # REST (confirm-delivery)
├── domain/          # Transaction, TransactionHistory
├── kafka/           # 5 consumers + producer
├── service/         # TransactionService, StateMachine, History
└── worker/          # TransactionTimeoutWorker
src/test/java/.../integration/   # Testes E2E de fluxo completo
db/init/             # Schema PostgreSQL
```
