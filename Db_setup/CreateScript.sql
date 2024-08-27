CREATE DATABASE logging ON
(NAME = test_dat,
    FILENAME = '/var/opt/mssql/data/logging.mdf',
    SIZE = 10,
    MAXSIZE = 50,
    FILEGROWTH = 5)

CREATE TABLE bronze_logging (
    timestamp DATETIME2,
    table NVARCHAR(255),
    comment NVARCHAR(255)
);

CREATE TABLE [dbo].[product] (
    [code]           NVARCHAR (50)  NULL,
    [direction]      NVARCHAR (50)  NULL,
    [display_name]   INT            NULL,
    [description]    NVARCHAR (MAX) NULL,
    [is_variable]    BINARY (50)    NULL,
    [is_green]       BINARY (50)    NULL,
    [is_tracker]     BINARY (50)    NULL,
    [is_prepay]      BINARY (50)    NULL,
    [is_business]    BINARY (50)    NULL,
    [is_restricted]  BINARY (50)    NULL,
    [term]           INT            NULL,
    [available_from] DATETIME2 (7)  NULL,
    [available_to]   DATETIME2 (7)  NULL,
    [links]          NVARCHAR (MAX) NULL
);
