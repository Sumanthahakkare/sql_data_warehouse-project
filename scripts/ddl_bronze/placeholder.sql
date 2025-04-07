/*
======================================================================
DDL Scripts : Create Bronze layer
=====================================================================
script purpose:
             1)this script create table in the 'bronze' schema
             2)run this script to redifine the ddl structure of 'bronze, tables 
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @STARTTIME DATETIME, @ENDTIME DATETIME
     BEGIN TRY
PRINT'=====================================';
PRINT'    CREATE BRONZE LAYER              '
PRINT'=====================================';
PRINT'=====================================';
PRINT'   LOADING CRM TABLE                  '
PRINT'=====================================';

SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.crm_cust_info;

PRINT'------------------------------------';
PRINT'      crm_Cust_info               '
PRINT'------------------------------------';


BULK INSERT bronze.crm_Cust_info
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\cust_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,  -- If the first row is a header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';

PRINT'------------------------------------';
PRINT'       crm_prd_info               '
PRINT'------------------------------------';



SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\prd_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,  -- If the first row is a header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';

PRINT'------------------------------------';
PRINT'       crm_sales_details               ';
PRINT'------------------------------------';


SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.crm_sales_details;
ALTER TABLE bronze.crm_sales_details
ALTER COLUMN sls_ord_num VARCHAR(50);  -- Adjust length as needed


BULK INSERT bronze.crm_sales_details
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\sales_details.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
   
);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';

PRINT'====================================';
PRINT'      CREATE ERP TABLE              '
PRINT'====================================';


PRINT'------------------------------------';
PRINT'      erp_LOC_A101             '
PRINT'------------------------------------';

SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.erp_LOC_A101;

BULK INSERT bronze.erp_LOC_A101
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\LOC_A101.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
   
);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';

PRINT'------------------------------------';
PRINT'       erp_CUST_AZ12               '
PRINT'------------------------------------';

SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.erp_CUST_AZ12;
BULK INSERT bronze.erp_CUST_AZ12
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\CUST_AZ12.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
   
);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';

PRINT'-------------------------------------';
PRINT'       erp_PX_CAT_G1V2               ';
PRINT'------------------------------------';


SET @STARTTIME=GETDATE()
TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

BULK INSERT bronze.erp_PX_CAT_G1V2
FROM 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Microsoft SQL Server Tools 19\PX_CAT_G1V2.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK

);
SET @ENDTIME=GETDATE()
PRINT'>>>>Load Duration:'+CAST(datediff(second,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS';
    END TRY 
	BEGIN CATCH 
	PRINT'============================================'
	PRINT'ERROR OCUURED DURING LOADING BRONZE LAYER'
	PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
	PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'============================================'
	END CATCH 

END
