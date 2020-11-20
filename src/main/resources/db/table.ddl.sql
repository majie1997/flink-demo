DROP TABLE IF EXISTS TRADE;

CREATE TABLE IF NOT EXISTS TRADE(
    tradeId int,
    tradeVersion int,
    cusip varchar(255),
    amount double,
    price double,
    createDate TIMESTAMP,
    processDate TIMESTAMP
);