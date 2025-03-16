/*

============================================================================
create database and schemas
============================================================================
script purpose:
      This script create new database called 'DataWarehouse'.Additionally, the script setup
      three schema with in the database: 'bronze','silver', and 'gold'
*/

USE master;
GO
--Create the 'DataWarehouse' database
  
CREATE DATABASE DataWarehouse;
GO
  
USE DataWarehouse;

--Create schemas 

CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
