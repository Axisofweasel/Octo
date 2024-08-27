-- Create a new database
CREATE DATABASE LoggingDB;

-- Use the new database
USE LoggingDB;

-- Create a new table
CREATE TABLE Logging (
    Timestamp,
    Database NVARCHAR(50),
    Table NVARCHAR(50),
    Message NVARCHAR(100)
);

-- Create a new table
CREATE TABLE Metadata (
    Timestamp,
    Database NVARCHAR(50),
    Table NVARCHAR(50),
    LastAppend INT,
    LoadType NVARCHAR(10),
    MergeColumns NVARCHAR(50)
);

CREATE DATABASE OctoBronzeDB

USE LoggingDB;

CREATE TABLE gas (
    Timestamp
    Reading AS DECIMAL(10,2),
    MD_Bronze_timestamp DATETIME
)

