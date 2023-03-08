select
    custmktsegment,
    sum(orderpartquantity) as totalqty,
    sum(orderpartprice) as totalprice,
    avg(ordercycletime) as avgcycletime
from {{ ref("sdm_ordersdetail") }}
where returnflag = 'N'
group by
    custmktsegment
order by
    custmktsegment
