/*

Enter custom T-SQL here that would run after SQL Server has started up. 

*/

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '$(MSSQL_DATABASE)')
BEGIN
    CREATE DATABASE $(MSSQL_DATABASE);

    CREATE LOGIN $(MSSQL_USER) WITH PASSWORD = '$(MSSQL_PASSWORD)';
END
GO

-- Switch to the newly created database
USE $(MSSQL_DATABASE);
GO

-- Create a database user and map it to the login
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '$(MSSQL_USER)')
BEGIN
    CREATE USER $(MSSQL_USER) FOR LOGIN $(MSSQL_USER);
    
    -- Grant database permissions
    GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE VIEW, CREATE SCHEMA TO $(MSSQL_USER);
END
GO
