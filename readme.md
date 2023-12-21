# flink-sql

This code is to process large-scale streaming data and execute SQL queries in real-time using Apache Flink. The query answer is refreshed in real-time as new data arrives in the form of a stream.

The background paper is(replication):

- A demo system for SQL processing over Flink: https://cse.hkust.edu.hk/~yike/Cquirrel.pdf

- Full paper: https://cse.hkust.edu.hk/~yike/sigmod20.pdf

This code implements Query 10 (a bit different) from the TPC-H benchmark, and the query statement is presented below. The purpose of Q10 is to retrieve information about customers and the losses incurred due to shipping issues in each country, specifically within a three-year timeframe starting from a specific date.

```sql
select
	c_custkey, c_name, 
	sum(l_extendedprice * (1 - l_discount)) as revenue, 
	c_acctbal,
	n_name, c_address, c_phone, c_comment 
from
	customer, orders, lineitem, nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-3-15' 
	and o_orderdate < date '1996-3-15' 
	and l_returnflag = 'R' 
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
```



