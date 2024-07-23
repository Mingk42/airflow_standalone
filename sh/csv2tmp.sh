#!/bin/bash

CSV_PATH=$1

user="root"
password="qwer123"
database="cmd_history"

mysql -u"$user" -p"$password" "$database" <<EOF
LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
-- LOAD DATA INFILE '${CSV_PATH}'
INTO TABLE cmd_history.cmd_usage
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
EOF
