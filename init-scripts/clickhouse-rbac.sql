-- Minimal ClickHouse RBAC bootstrap for Trusted Zone.
-- Existing admin/default/analytics accounts are preserved for maintenance.

CREATE DATABASE IF NOT EXISTS bi_analytics;

CREATE USER IF NOT EXISTS trusted_structured_writer
IDENTIFIED WITH plaintext_password BY 'trusted_structured_writer_password';

CREATE ROLE IF NOT EXISTS trusted_structured_writer_role;

GRANT SHOW TABLES, SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER
ON bi_analytics.*
TO trusted_structured_writer_role;

GRANT trusted_structured_writer_role TO trusted_structured_writer;

CREATE USER IF NOT EXISTS trusted_structured_reader
IDENTIFIED WITH plaintext_password BY 'trusted_structured_reader_password';

CREATE ROLE IF NOT EXISTS trusted_structured_reader_role;

GRANT SHOW TABLES, SELECT
ON bi_analytics.*
TO trusted_structured_reader_role;

GRANT trusted_structured_reader_role TO trusted_structured_reader;
