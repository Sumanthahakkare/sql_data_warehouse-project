/*
--------------------------------------------------------------------------------------
 View Name      : gold_fact_sales
 Layer          : Gold (Fact Table - Reporting Layer)
 Description    : 
     This view represents the Sales Fact Table, joining transactional sales data 
     with customer and product dimensions. It serves as a central fact table for 
     sales reporting and analysis.

 Key Logic:
     - Source data from 'crm_sales_details'.
     - Joins with gold_dimension_product using product key.
     - Joins with gold_dimension_customer using customer ID.
     - Includes order, ship, due dates and key sales metrics.

 Notes:
     - Ensure that gold_dimension_product.category_id is the correct join 
       for cs.sls_prd_key. You might need to join on product_key instead.

 Created By     : [Your Name]
 Created On     : [Date]
*/



create view gold_fact_sales as 
select 
cs.sls_ord_num as order_number,
dc.customer_id,
gd.product_key,
dc.CUSTOMER_KEY as customer_key,
cs.sls_order_dt as order_date,
cs.sls_ship_dt as ship_date,
cs.sls_due_dt as  due_date,
cs.sls_sales  as sales,	
cs.sls_quantity as quantity,
cs.sls_price as price 
from silver.crm_sales_details cs
left join gold_dimension_product gd
on cs.sls_prd_key=gd.category_id
left join gold_dimension_customer dc
on cs.sls_cust_id=dc.customer_id
