CREATE TABLE items (
    id         bigserial     PRIMARY KEY,
    name       text          NOT NULL,
    price      numeric       NOT NULL,
    stock      int           NOT NULL CHECK (stock >= 0),
    created_at timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id   text          PRIMARY KEY,
    item_id    bigint        NOT NULL REFERENCES items (id),
    status     text          NOT NULL,
    created_at timestamptz   NOT NULL DEFAULT now()
);
