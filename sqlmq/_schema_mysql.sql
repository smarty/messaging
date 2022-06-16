CREATE TABLE Messages (
    id         bigint unsigned AUTO_INCREMENT NOT NULL,
    dispatched datetime(3)                        NULL,
    type       varchar(256)                   NOT NULL,
    payload    mediumblob                     NOT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_messages_dispatched ON Messages (dispatched, id);
