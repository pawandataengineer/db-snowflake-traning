
select 

---from orders
o.orderid ,
o.orderdate,
o.shipdate,
o.shipmode,
(o.ordersellingprice -o.ordercostprice ) orderprofit ,

-- from raw customer
c.customername ,
c.segment ,
c.country ,
c.state ,

--from raw product
p.category ,
p.subcategory ,
p.productname

from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid