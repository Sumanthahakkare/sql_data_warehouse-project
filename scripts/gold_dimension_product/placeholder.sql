/*
--------------------------------------------------------------------------------------
 View Name      : gold_dimension_product
 Layer          : Gold (Reporting Layer)
 Description    : 
     This view filters out historical (closed) records from the 'crm_prd_info' table 
     and retains only active product transactions (where prd_end_dt is NULL). 
     It enriches the product data with category and maintenance information 
     from 'erp_PX_CAT_G1V2'.

 Key Logic:
     - Filters to include only records with NULL prd_end_dt (open/active).
     - Performs LEFT JOIN on product key to bring in category/subcategory info.
     - Outputs analytics-ready product dimension data.
*/



create view gold_dimension_product as
select 
pn.prd_id as product_id,
pn.prd_key AS product_key,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pn.prd_nm as product_name,
pc.MAINTENANCE as maintenance,
pn.prd_cost as product_cost,	
pn.prd_line as product_line,	
pn.prd_start_dt  as start_date	 
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
on pn.prd_key=pc.ID
where prd_end_dt is null
