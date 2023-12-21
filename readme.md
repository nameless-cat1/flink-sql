# flink-sql

This code is to process large-scale streaming data and execute SQL queries in real-time using Apache Flink. The query answer is refreshed in real-time as new data arrives in the form of a stream.

The background paper is(replication):

- A demo system for SQL processing over Flink: https://cse.hkust.edu.hk/~yike/Cquirrel.pdf

- Full paper: https://cse.hkust.edu.hk/~yike/sigmod20.pdf

This code implements Query 10 (a bit different) from the TPC-H benchmark, and the query statement is presented below. The purpose of Q10 is to retrieve information about customers and the losses incurred due to shipping issues in each country, specifically within a three-year timeframe starting from a specific date.

![image-20231221104214343](.\img\img.png)

