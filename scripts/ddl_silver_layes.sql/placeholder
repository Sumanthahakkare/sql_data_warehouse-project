/*
============================================================
  PROCEDURE NAME : silver.load_silver
  LAYER          : SILVER (CLEANED AND TRANSFORMED DATA)
  PURPOSE        : Load cleaned and standardized data 
                   from BRONZE to SILVER layer
============================================================

  🔹 STEPS PERFORMED:

  1. CRM CUSTOMER INFO (silver.crm_cust_info)
     - TRUNCATE table before loading
     - DEDUPLICATE using ROW_NUMBER() over cst_id 
       (keeping latest by cst_create_date)
     - TRIM first and last names
     - STANDARDIZE marital status:
         'S' → 'Single', 'M' → 'Married', else 'N/A'
     - STANDARDIZE gender:
         'F' → 'Female', 'M' → 'Male', else 'N/A'

  2. PRODUCT INFO (silver.crm_prd_info)
     - TRUNCATE table
     - SPLIT prd_key to extract cat_id and prd_key
     - HANDLE NULL product cost with ISNULL
     - MAP product line codes:
         'M' → 'Mountain', 'R' → 'Road', etc.
     - GENERATE prd_end_dt using LEAD()

  3. SALES DETAILS (silver.crm_sales_details)
     - TRUNCATE table
     - VALIDATE dates (e.g., invalid 0 or malformed entries → NULL)
     - RECALCULATE sales if inconsistent with quantity × price
     - ADJUST prices to be POSITIVE if needed

  4. CUSTOMER MASTER (silver.erp_CUST_AZ12)
     - TRUNCATE table
     - REMOVE prefix 'NAS' from CID
     - NULLIFY future birth dates
     - STANDARDIZE gender values

  5. LOCATION MASTER (silver.erp_LOC_A101)
     - TRUNCATE table
     - REMOVE dashes from CID
     - MAP country codes:
         'DE' → 'Germany', 'US'/'USA' → 'United States'

  6. CATEGORY MASTER (silver.erp_PX_CAT_G1V2)
     - TRUNCATE table
     - TRIM ID, CAT, SUBCAT fields
     - LOAD MAINTENANCE column as-is

------------------------------------------------------------
  🔹 ERROR HANDLING:
     - Wrapped in TRY...CATCH block
     - LOG meaningful ERROR messages using PRINT
     - Captures and prints ERROR_MESSAGE() and ERROR_NUMBER()

  🔹 EXECUTION LOGGING:
     - CAPTURES start and end time for each section
     - PRINTS total execution time in SECONDS for monitoring
============================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @STARTTIME DATETIME, @ENDTIME DATETIME;
 BEGIN TRY
SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT' truncate table silver.crm_cust_info '
PRINT'=====================================';
truncate table silver.crm_cust_info;
PRINT'=====================================';
PRINT' Insert into silver.crm_cust_info '
PRINT'=====================================';

Insert into silver.crm_cust_info(
cst_id ,
cst_key ,
cst_firstname ,
cst_lastname ,	
cst_marital_status ,
cst_gndr ,
cst_create_date )

select
cst_id ,
cst_key ,
trim(cst_firstname) as cst_firstname ,
trim(cst_lastname)as cst_lastname,	
CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	 ELSE 'N/A'
End cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'N/A'
END cst_gndr,
cst_create_date 
FROM(
SELECT * ,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS FLAG 
FROM bronze.crm_cust_info)t where flag =1
SET @ENDTIME=GETDATE()
PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);


SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT' truncate table silver.crm_prd_info '
PRINT'=====================================';
truncate table silver.crm_prd_info;
PRINT'=====================================';
PRINT' Insert into silver.crm_prd_info '
PRINT'=====================================';

INSERT INTO silver.crm_prd_info(
prd_id ,
prd_key	,
cat_id ,
prd_nm,
prd_cost,	
prd_line ,	
prd_start_dt,	
prd_end_dt )
select 
prd_id ,
replace(substring(prd_key,1,5),'-','_')as cat_id,
substring(prd_key,7,len(prd_key))as prd_key,
prd_nm	,
isnull(prd_cost ,0) as prd_cost,	
case upper(trim(prd_line))
     when 'M' THEN 'Mountain'
	 when 'R' THEN 'Road'
	 when 'S' THEN 'Ohter Sales'
	 when 'T' THEN 'Touring'
	 Else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt ,	
cast(lead(prd_start_dt)over(partition by prd_key order by prd_start_dt)-1 as date )as prd_end_date 
from bronze.crm_prd_info
SET @ENDTIME=GETDATE()
PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);


SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT'truncate table silver.crm_sales_details '
PRINT'=====================================';
truncate table silver.crm_sales_details;
PRINT'=====================================';
PRINT' Insert into silver.crm_sales_details '
PRINT'=====================================';

INSERT INTO silver.crm_sales_details (
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt	,
sls_due_dt,
sls_sales ,	
sls_quantity ,
sls_price )
select 
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
case 
    when sls_order_dt =0 or len(sls_order_dt)!=8 or sls_order_dt<0 then null
	else cast(cast(sls_order_dt as varchar) as date)
	end as sls_order_dt ,
case 
    when sls_ship_dt =0 or len(sls_ship_dt)!=8 or sls_ship_dt<0 then null
	else cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt ,
case 
    when sls_due_dt =0 or len(sls_due_dt)!=8 or sls_due_dt<0 then null
	else cast(cast(sls_due_dt as varchar) as date)
	end as sls_due_dt ,
case 
	    when sls_sales <0 or sls_sales is null or sls_sales!=sls_quantity*sls_price
		then sls_quantity*abs(sls_price)
		else sls_sales
		end as sls_sales,			
sls_quantity ,
case 
	    when sls_price<0 or sls_price is null or sls_price!=sls_sales/nullif(sls_quantity,0)
		then abs(sls_sales)/nullif(sls_quantity,0) 
		else sls_price
		end as sls_price 
from bronze.crm_sales_details
SET @ENDTIME=GETDATE()
PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);


SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT' truncate table silver.erp_CUST_AZ12 '
PRINT'=====================================';
truncate table silver.erp_CUST_AZ12;
PRINT'=====================================';
PRINT' Insert into silver.erp_CUST_AZ12 '
PRINT'=====================================';


truncate table silver.erp_CUST_AZ12
INSERT INTO silver.erp_CUST_AZ12(
CID ,
BDATE ,
GEN )

SELECT
		CASE WHEN CID LIKE'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID 
		END CID ,
CASE 
    WHEN BDATE> GETDATE() THEN NULL
	ELSE BDATE
	END AS BDATE ,
CASE WHEN  UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		     WHEN  UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
			 ELSE 'N/A' 
			 END AS GEN
 FROM BRONZE.erp_CUST_AZ12
 SET @ENDTIME=GETDATE()
 PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);



 SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT' truncate table silver.erp_LOC_A101 '
PRINT'=====================================';
truncate table silver.erp_LOC_A101;
PRINT'=====================================';
PRINT' Insert into silver.erp_LOC_A101 '
PRINT'=====================================';

 insert into silver.erp_LOC_A101(
CID ,
CNTRY )
 select 
replace(cid,'-','') as cid ,
case when trim(cntry) ='DE' then 'Germany'
     when trim (cntry) in ('US','USA') THEN 'United states'
	 when trim (cntry) ='' or CNTRY is null then 'n/a'
	 else trim(cntry)
	 end as cntry 
	from bronze.erp_LOC_A101
	SET @ENDTIME=GETDATE()
PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);


SET @STARTTIME=GETDATE()
PRINT'=====================================';
PRINT' truncate table silver.erp_PX_CAT_G1V2 '
PRINT'=====================================';
truncate table silver.erp_PX_CAT_G1V2;
PRINT'=====================================';
PRINT' Insert intosilver.erp_PX_CAT_G1V2 '
PRINT'=====================================';

truncate table silver.erp_PX_CAT_G1V2
insert into  silver.erp_PX_CAT_G1V2(
ID ,
CAT	,
SUBCAT ,
MAINTENANCE)
select
trim(id) as id,
trim(cat) as cat,
trim(subcat) as subcat ,
MAINTENANCE 
from bronze.erp_PX_CAT_G1V2
SET @ENDTIME=GETDATE()
PRINT 'Total Execution Time (Seconds): ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR);
 END TRY 
	BEGIN CATCH 
	PRINT'============================================'
	PRINT'ERROR OCUURED DURING LOADING SILVER LAYER'
	PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
	PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'============================================'
	END CATCH 

END
