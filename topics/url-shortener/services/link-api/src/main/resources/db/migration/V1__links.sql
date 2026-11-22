CREATE SEQUENCE link_id_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE links (
    id         bigint      PRIMARY KEY DEFAULT nextval('link_id_seq'),
    code       text        NOT NULL UNIQUE,
    url        text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE link_clicks (
    code   text   PRIMARY KEY,
    clicks bigint NOT NULL DEFAULT 0
);
