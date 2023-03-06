select
    parttype,
    sum(case when returnflag='N' then orderpartquantity else 0 end) as totalqty_noreturn,
    sum(case when returnflag!='N' then orderpartquantity else 0 end) as totalqty_return,
    sum(case when returnflag='N' then orderpartprice else 0 end) as totalprice_noreturn,
    sum(case when returnflag!='N' then orderpartprice else 0 end) as totalprice_return,
    sum(case when returnflag='N' then ordercycletime else 0 end) / count(case when returnflag='N' then 1 else 0 end) as avgcycletime_noreturn,
    sum(case when returnflag!='N' then ordercycletime else 0 end) / count(case when returnflag!='N' then 1 else 0 end) as avgcycletime_return
from {{ ref("sdm_ordersdetail") }}
group by
    parttype

