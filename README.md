# IEC_dbt_training_test

> PSG-DG-Q1_Sharing_DBT_v1.0
> 
> DBT Exercise Demo Code
> 
> Created Date: 2023/03/08
> 
> Author: Silvia Yang, Jesse Wei


[TOC]

<!-- 
Table of Content
1. Setup
2. Prepare Dataset
3. Start Project
4. Exercise
    4.1 Integrate raw data to dbt
    4.2 Create intermediate data model
    4.3 Create reporting/analytics data model
    4.4 Configuration and test
    4.5 Documentation
-->


# 1. Setup

1. [Snowflake Signup](https://signup.snowflake.com/)
2. [dbt signup](https://getdbt.com/)
3. [github signup](https://github.com/)


# 2. Prepare Dataset

Create a Snowflake Worksheet **IEC_TRN: Initial dataset**

```sql

CREATE TABLE if not exists raw.tpch_sf1.customer LIKE snowflake_sample_data.tpch_sf1.customer;
CREATE TABLE if not exists raw.tpch_sf1.lineitem LIKE snowflake_sample_data.tpch_sf1.lineitem;
CREATE TABLE if not exists raw.tpch_sf1.nation LIKE snowflake_sample_data.tpch_sf1.nation;
CREATE TABLE if not exists raw.tpch_sf1.orders LIKE snowflake_sample_data.tpch_sf1.orders;
CREATE TABLE if not exists raw.tpch_sf1.part LIKE snowflake_sample_data.tpch_sf1.part;
CREATE TABLE if not exists raw.tpch_sf1.partsupp LIKE snowflake_sample_data.tpch_sf1.partsupp;
CREATE TABLE if not exists raw.tpch_sf1.region LIKE snowflake_sample_data.tpch_sf1.region;
CREATE TABLE if not exists raw.tpch_sf1.supplier LIKE snowflake_sample_data.tpch_sf1.supplier;

insert into raw.tpch_sf1.customer select * from snowflake_sample_data.tpch_sf1.customer;
insert into raw.tpch_sf1.lineitem select * from snowflake_sample_data.tpch_sf1.lineitem;
insert into raw.tpch_sf1.nation select * from snowflake_sample_data.tpch_sf1.nation;
insert into raw.tpch_sf1.orders select * from snowflake_sample_data.tpch_sf1.orders;
insert into raw.tpch_sf1.part select * from snowflake_sample_data.tpch_sf1.part;
insert into raw.tpch_sf1.partsupp select * from snowflake_sample_data.tpch_sf1.partsupp;
insert into raw.tpch_sf1.region select * from snowflake_sample_data.tpch_sf1.region;
insert into raw.tpch_sf1.supplier select * from snowflake_sample_data.tpch_sf1.supplier;

select top 30 * from raw.tpch_sf1.lineitem;

```


# 3. Start Project

Configure dbt project environment

- Name: dbt_training
- Account: `YOUR_SNOWFLAKE_ACCOUNT`
- Database: ANALYTICS
- Warehouse: IEC_DBT_TRAIN
- Username: `YOUR_SNOWFLAKE_USERNAME`
- Password: `YOUR_SNOWFLAKE_PASSWORD`

Create github public repo

- Repositry Name: IEC_DBT_TRAIN

Initialize dbt IDE

- Create branch: analytics_rpt
- run dbt command `dbt run`


# 4. Exercise

## 4.1 Integrate raw data to dbt

- raw_customer.sql

```sql
select * from raw.tpch_sf1.customer
```

- raw_lineitem.sql

```sql
select * from raw.tpch_sf1.lineitem
```

- raw_nation.sql

```sql
select * from raw.tpch_sf1.nation
```

- raw_orders.sql

```sql
select * from raw.tpch_sf1.orders
```

- raw_part.sql

```sql
select * from raw.tpch_sf1.part
```

- raw_region.sql

```sql
select * from raw.tpch_sf1.region
```

- In file dbt_project.yml, modify as below 

```yml
models:
  my_new_project:
    # Applies to all files under models/example/
    example:
      +materialized: view
    raw:
      +materialized: table

```

- run dbt command `dbt run --models raw.*`


## 4.2 Create intermediate data model

- sdm_orderdetail.sql

```sql
select
    l.l_orderkey as orderkey,
    l.l_returnflag as returnflag,
    o.o_orderdate as orderdate,
    l.l_shipdate as shipdate,
    l.l_partkey as partkey,
    p.p_type as parttype,
    c.c_custkey as custkey,
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
    l.l_returnflag,
    o.o_orderdate,
    l.l_shipdate,
    l.l_partkey,
    p.p_type,
    c.c_custkey,
    c.c_mktsegment,
    n.n_name,
    r.r_name

```

- In file dbt_project.yml, modify as below 

```yml
models:
  my_new_project:
    # Applies to all files under models/example/
    example:
      +materialized: view
    raw:
      +materialized: table
    intermediate:
      +materialized: table
```

- run dbt command `dbt run --models sdm_ordersdetail`


## 4.3 Create reporting/analytics data model

- rpt_price_by_return.sql
```sql
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
{{ ref("raw_lineitem") }}
WHERE
l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
GROUP BY
l_returnflag, l_linestatus
ORDER BY
l_returnflag, l_linestatus

```

- rpt_sales_by_cust.sql

```sql
select
    -- TRUNC(orderdate, 'Month') AS ordermonth,
    -- region,
    custmktsegment,
    sum(orderpartquantity) as totalqty,
    sum(orderpartprice) as totalprice,
    avg(ordercycletime) as avgcycletime
from {{ ref("sdm_ordersdetail") }}
where returnflag = 'N'
group by
    -- TRUNC(orderdate, 'Month')
    -- region,
    custmktsegment
order by
    -- TRUNC(orderdate, 'Month')
    -- region,
    custmktsegment

```

- rpt_sales_by_part.sql

```sql
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

```

- In file dbt_project.yml, modify as below 

```yml
models:
  my_new_project:
    # Applies to all files under models/example/
    example:
      +materialized: view
    raw:
      +materialized: table
    intermediate:
      +materialized: table
    reporting:
      +materialized: table

```

- run dbt command `dbt run --models reporting.*`


## 4.4 Configuration and test

- raw_tpch_sf1.yml

```yml
version: 2

models:
  - name: raw_customer
    columns:
      - name: C_CUSTKEY
        tests:
          - unique
          - not_null
      - name: C_MKTSEGMENT
        tests:
          - not_null

  - name: raw_lineitem
    columns:
      - name: L_ORDERKEY
        tests:
          - not_null
      - name: L_PARTKEY
        tests:
          - not_null
      - name: L_RETURNFLAG
        tests:
          - accepted_values:
              values: ['R', 'N', 'A']

  - name: raw_orders
    columns:
      - name: O_ORDERKEY
        tests:
          - unique
          - not_null
      - name: O_CUSTKEY
        tests:
          - not_null

```

- run dbt command `dbt test --select raw_customer`


## 4.5 Documentation

- raw_tpch_sf1.yml

```yml
version: 2

models:
  - name: raw_customer
    description: Staged customer data from snowflake sample data.
    columns:
      - name: C_CUSTKEY
        description: The primary key for customers.
        tests:
          - unique
          - not_null
      - name: C_MKTSEGMENT
        description: The reporting dimension for customers.
        tests:
          - not_null

  - name: raw_lineitem
    description: Staged lineitem data from snowflake sample data.
    columns:
      - name: L_ORDERKEY
        description: The foreign key for lineitem and orders.
        tests:
          - not_null
      - name: L_PARTKEY
        description: The foreign key for lineitem and parts.
        tests:
          - not_null
      - name: L_RETURNFLAG
        description: "{{ doc('returnflag') }}"
        tests:
          - accepted_values:
              values: ['R', 'N', 'A']

  - name: raw_orders
    description: Staged orders data from snowflake sample data.
    columns:
      - name: O_ORDERKEY
        description: The primary key for orders.
        tests:
          - unique
          - not_null
      - name: O_CUSTKEY
        description: The foreign key for orders and customers.
        tests:
          - not_null

```

- returnflag_tpch_sf1.md

```md
{% docs returnflag %}
	
One of the following values: 

| flag   | definition                                                       |
|--------|------------------------------------------------------------------|
| N      | Order placed, no return                                          |
| A      | Order has been returned by customers, not yet been delivered     |
| R      | Order has been returned by customers and been received by plant  |

{% enddocs %}
```

- run dbt command `dbt docs generate`


