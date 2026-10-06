CREATE DATABASE IF NOT EXISTS toll_db;

USE toll_db;

CREATE TABLE IF NOT EXISTS toll_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id VARCHAR(50),
    vehicle_type VARCHAR(50),
    toll_plaza_id VARCHAR(50),
    timestamp DATETIME
);