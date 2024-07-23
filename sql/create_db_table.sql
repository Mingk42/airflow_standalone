CREATE DATABASE IF NOT EXISTS history_db;

USE history_db;

CREATE TABLE IF NOT EXISTS history_db.tmp_cmd_usage (
	dt VARCHAR(20),
	command VARCHAR(500),
	cnt VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS history_db.cmd_usage (
	dt DATE,
	command VARCHAR(500),
	cnt INT,
	tmp_dt VARCHAR(500)
);
