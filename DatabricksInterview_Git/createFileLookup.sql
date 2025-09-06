CREATE TABLE IF NOT EXISTS dbinterview.source.fileLookup
(
  file STRING,
  type STRING
);

delete from dbinterview.source.fileLookup;

INSERT INTO dbinterview.source.fileLookup
VALUES
('productsOrders','.csv'),
('orders_day1','.csv');




--update dbinterview.source.fileLookup set file = 'orders_day1' where file = 'orders_day1.csv';
