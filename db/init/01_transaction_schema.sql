-- Schema exclusivo do transaction-service
CREATE SCHEMA IF NOT EXISTS transaction_service;

-- Sequence para transactions.id
CREATE SEQUENCE IF NOT EXISTS transaction_service.transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para transaction_history.id
CREATE SEQUENCE IF NOT EXISTS transaction_service.transaction_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Tabela principal: estado atual da transação
CREATE TABLE IF NOT EXISTS transaction_service.transactions (
    id                  BIGINT PRIMARY KEY DEFAULT nextval('transaction_service.transactions_id_seq'),
    correlation_id      UUID         NOT NULL,
    auction_id          BIGINT       NOT NULL,
    buyer_id            UUID         NOT NULL,
    seller_id           UUID         NOT NULL,
    payment_id          UUID         NULL,
    winner_bid_value    NUMERIC(19, 2) NOT NULL,
    amount_in_cents     INTEGER      NOT NULL,
    status              VARCHAR(50)  NOT NULL,
    created_at          TIMESTAMP    NOT NULL,
    updated_at          TIMESTAMP    NOT NULL,
    expires_at          TIMESTAMP    NOT NULL,
    CONSTRAINT uk_transactions_correlation_id UNIQUE (correlation_id)
);

-- Tabela de auditoria: histórico de mudanças de status
CREATE TABLE IF NOT EXISTS transaction_service.transaction_history (
    id              BIGINT PRIMARY KEY DEFAULT nextval('transaction_service.transaction_history_id_seq'),
    transaction_id  BIGINT       NOT NULL,
    old_status      VARCHAR(50)  NULL,
    new_status      VARCHAR(50)  NOT NULL,
    changed_by      VARCHAR(20)  NOT NULL,
    reason          VARCHAR(500) NULL,
    occurred_at     TIMESTAMP    NOT NULL,
    CONSTRAINT fk_history_transaction
        FOREIGN KEY (transaction_id) REFERENCES transaction_service.transactions (id)
);

-- Índices para workers e consultas
CREATE INDEX IF NOT EXISTS idx_transactions_status_expires_at
    ON transaction_service.transactions (status, expires_at);

CREATE INDEX IF NOT EXISTS idx_transactions_created_at
    ON transaction_service.transactions (created_at);

CREATE INDEX IF NOT EXISTS idx_transactions_status
    ON transaction_service.transactions (status);

CREATE INDEX IF NOT EXISTS idx_history_transaction_id
    ON transaction_service.transaction_history (transaction_id);
