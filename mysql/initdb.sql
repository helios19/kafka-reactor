GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'debezium' IDENTIFIED BY 'dbz';

--create database test;
--use test;
--
--CREATE TABLE raw-transaction
--(
--id INTEGER AUTO_INCREMENT,
--name TEXT,
--PRIMARY KEY (id)
--) COMMENT='this is my test table';