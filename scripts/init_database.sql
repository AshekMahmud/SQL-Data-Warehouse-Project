/*
----------------------------
Create Database and Schema
----------------------------
Script Purpose:
	This script create a new database "Data_Warehouse" after checking if it already exists.
	If the database exists, it will droped and recreated. Additionally this script create new
	schemas withing the database "Data_Warehouse". They are 'first_layer', 'second_layer' and 'third_layer'.

WARNING:
	Running this script will drop the entire "Data_Warehouse" database if it exists. All data will be
	permanently deleted. Proced with caution and ensure you have proper backups before running the
	script.
*/
-- create new database

USE MASTER;

-- Drop and recreate the 'Data_Warehouse' database

--  Check if the database exists
IF EXISTS (
    SELECT NAME 
    FROM sys.databases 
    WHERE name = 'Data_Warehouse'
)
BEGIN
    -- Set to SINGLE_USER to disconnect active sessions
    ALTER DATABASE Data_Warehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE Data_Warehouse;
END
GO

-- Create the new database
CREATE DATABASE Data_Warehouse;
GO

USE Data_Warehouse;

-- Create Schemas
CREATE SCHEMA first_layer;
GO

CREATE SCHEMA second_layer;
GO

CREATE SCHEMA third_layer;
GO
