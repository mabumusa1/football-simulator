#!/bin/bash
set -e

clickhouse-client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS football_simulator;
EOSQL

echo "Database football_simulator created successfully"
