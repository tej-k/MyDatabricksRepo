  -- CREATE TABLE db_jobs.default.orders(
  --   id INT,
  --   customer_id INT,
  --   order_date DATE,
  --   status STRING,
  --   total DECIMAL(10,2)
  -- );

  -- INSERT INTO db_jobs.default.orders VALUES
  --   (1, 101, '2022-01-01', 'shipped', 50.00),
  --   (2, 102, '2022-01-02', 'shipped', 75.00),
  --   (3, 103, '2022-01-03', 'shipped', 100.00),
  --   (4, 104, '2022-01-04', 'shipped', 125.00);













  select * from db_jobs.default.orders where id = :var_id;




