select
    l.l_orderkey as orderkey,
    o.o_orderdate as orderdate,
    l.l_shipdate as shipdate,
    l.l_partkey as partkey,
    p.p_type as parttype,
    p.p_brand as partbrand,
    c.c_custkey as custkey,
    c.c_name as custname,
    c.c_mktsegment as custmktsegment,
    n.n_name as nation,
    r.r_name as region,
    sum(l.l_quantity) as orderpartquantity,
    sum(l.l_extendedprice) as orderpartprice,
    datediff(days, o.o_orderdate, l.l_shipdate) as ordercycletime
from {{ ref("raw_lineitem") }} as l
left join {{ ref("raw_part") }} as p on l.l_partkey = p.p_partkey
left join {{ ref("raw_orders") }} as o on l.l_orderkey = o.o_orderkey
left join {{ ref("raw_customer") }} as c on o.o_custkey = c.c_custkey
left join {{ ref("raw_nation") }} as n on c.c_nationkey = n.n_nationkey
left join {{ ref("raw_region") }} as r on n.n_regionkey = r.r_regionkey
group by
    l.l_orderkey,
    o.o_orderdate,
    l.l_shipdate,
    l.l_partkey,
    p.p_type,
    p.p_brand,
    c.c_custkey,
    c.c_name,
    c.c_mktsegment,
    n.n_name,
    r.r_name
