-- Go Worker Database Schema Setup
-- Run this on Computer B (Go Worker)

CREATE DATABASE IF NOT EXISTS worker_go_db;
USE worker_go_db;

-- Databases metadata table
CREATE TABLE IF NOT EXISTS `databases` (
    name VARCHAR(255) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tables metadata table
CREATE TABLE IF NOT EXISTS tables_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    database_name VARCHAR(255),
    table_name VARCHAR(255),
    schema_json JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_table (database_name, table_name),
    FOREIGN KEY (database_name) REFERENCES `databases`(name) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SHOW TABLES;