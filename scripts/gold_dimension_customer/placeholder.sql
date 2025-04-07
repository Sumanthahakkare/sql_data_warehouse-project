/*
--------------------------------------------------------------------------------------
 View Name      : gold_dimension_customer
 Layer          : Gold (Reporting Layer)
 Description    : 
     This view consolidates customer information from multiple source systems
     to provide a unified and enriched customer dimension table. It includes
     basic demographics such as name, marital status, gender, birth date, 
     and country.

 Key Logic:
     - Pulls primary customer info from 'crm_cust_info'.
     - Enriches gender data with 'erp_CUST_AZ12' when missing or marked as 'N/A'.
     - Joins with location data from 'erp_LOC_A101' for country info.
     - Ensures customer gender is set using fallback logic.
*/



CREATE VIEW gold_dimension_customer as 
select 
ci.cst_id  AS CUSTOMER_ID,
ci.cst_key AS CUSTOMER_KEY ,
ci.cst_firstname AS FIRST_NAME ,
ci.cst_lastname AS LAST_NAME ,	
ci.cst_marital_status AS MARITAL_STATUS ,
case when ci.cst_gndr!= 'N/A' then ci.cst_gndr
else coalesce( ca.gen,'N/A') end as GENDER ,
ci.cst_create_date AS CREATE_DATE ,
ca.bdate AS BIRT_DATE,
la.cntry AS COUNTRY
from silver.crm_cust_info as ci
left join silver.erp_CUST_AZ12 as ca
on ci.cst_key=ca.CID
left join silver.erp_LOC_A101 la
on ci.cst_key=la.CID
