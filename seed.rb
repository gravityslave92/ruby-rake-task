#!/usr/bin/evn ruby

require 'pg'

num_rows = 1_000_000

conn = PG.connect(
  host: 'localhost',
  port: 5432,
  user: 'timur',
  password: 'timur',
  dbname: 'test'
)

conn.exec <<-SQL
  CREATE TABLE IF NOT EXISTS randoms (
    id SERIAL PRIMARY KEY,
    random uuid
  );
SQL

conn.exec <<-SQL
  INSERT INTO randoms(random)
  SELECT null
  FROM generate_series(1, #{num_rows})
SQL

conn.close
