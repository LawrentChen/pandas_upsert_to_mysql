**Strongly suggest using [pangres](https://github.com/ThibTrip/pangres), which is much more efficient and elegant than this package.**



# pandas_upsert_to_mysql

Enhanced `to_sql` method in pandas DataFrame, for MySQL database only. It provides a relatively convenient **upsert** (insert or update) feature inplementation through temporary table.

- Whether a record needs to be update or not is determined by primary key or unique constraint
- The MySQL database table structure requires to be well designed (need to use SQLAlchemy)
- The primary key must be MySQL auto-increment. Not allow composite primary key mixing auto-increment and other columns



## Installation

```shell
pip install pandas_upsert_to_mysql
```



## Usage and Example

Let's use an order table as instance. Here the `row_id` is the auto-incremented primary key. `order_id` and `product_id` make up of the unique contraint (a single order can have more than one kind of product).

```mysql
-- Here we use native SQL to create the table for illustration convenience. In the actual pratice we need to use SQLAlchemy.

CREATE TABLE `order` (
  `row_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'auto_incremented_ID',
  `order_id` varchar(5) NOT NULL DEFAULT '-9999' COMMENT 'order_id',
  `product_id` varchar(5) NOT NULL DEFAULT '-9999' COMMENT 'product_id',
  `qty` int(11) DEFAULT NULL COMMENT 'purchase_quantity',
  `refund_qty` int(11) DEFAULT NULL COMMENT 'refund_quantity',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last_update_time',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'first_create_time',
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `main` (`order_id`,`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Order Info'
```

As time moving, this table needs two operations as below:

1. New orders: insert new records
2. Existed orders apply refund: update the `refund_qty` ('qty' for quantity) field of the specific orders



We insert the base data first. This step can be easily done with pandas official native method `to_sql`

| row_id | order_id | product_id |  qty | refund_qty | update_time         | create_time         |
| -----: | :------- | :--------- | ---: | ---------: | :------------------ | :------------------ |
|      1 | A0001    | PD100      |   10 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |
|      2 | A0002    | PD200      |   20 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |
|      3 | A0002    | PD201      |   22 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |

Assume the database already has the data above. Now we need to upsert the new data below. Pay attention that A0002-PD201 now has two refund_qty, and we have the new record A0003-PD300.

| order_id | product_id |  qty | refund_qty |
| :------- | :--------- | ---: | ---------: |
| A0001    | PD100      |   10 |          0 |
| A0002    | PD200      |   20 |          0 |
| A0002    | PD201      |   22 |          2 |
| A0003    | PD300      |   30 |          0 |



```python
# 'table' is a module containing pre-defined SQLAlchemy ORM table structure classes
# 'engine' is a sqlalchemy.engine.Engine which needs you to define yourself. We use it to connect to the target MySQL database. It has the same requirements with the paramater 'con' in pandas.DataFrame.to_sql
# Both 'table' and 'engine' here is just the instance in package. You should define them by yourself in actual usage

import pandas_upsert_to_mysql.table as table
from pandas_upsert_to_mysql import Upsert
from connection import Connector

engine = Connector(schema='dev').get_engine()

Upsert(engine=engine).to_mysql(df=table.ExampleOrderTable.new_df,
                               target_table=table.Order,
                               temp_table=table.OrderTemp,
                               if_record_exists='update')
```

Then we can get the result. Be aware that the `update_time` only changed in the record whose row_id=3, completely as expected.

| row_id | order_id | product_id |  qty | refund_qty | update_time             | create_time             |
| -----: | :------- | :--------- | ---: | ---------: | :---------------------- | :---------------------- |
|      1 | A0001    | PD100      |   10 |          0 | 2020-06-26 11:11:55     | 2020-06-26 11:11:55     |
|      2 | A0002    | PD200      |   20 |          0 | 2020-06-26 11:11:55     | 2020-06-26 11:11:55     |
|      3 | A0002    | PD201      |   22 |          2 | **2020-06-26 11:13:19** | 2020-06-26 11:11:55     |
|      4 | A0003    | PD300      |   30 |          0 | **2020-06-26 11:13:19** | **2020-06-26 11:13:19** |



## Caveats

- Any conlumn in the unique constraint must not have null value, otherwise the update will violate the constraint and cause duplicates. This is considered as a bug [#8173](https://bugs.mysql.com/bug.php?id=8173) of MySQL with long history, which means before we write the DataFrame to the database, we should assign a default value for each column in unique constraint to replace the possible null values. 
- Implement using `session scope` of SQLAlchemy. But since the author is not a professional engineer, I still can not gurantee the reliability in concurrent condition
- Only tested in pandas >= 1.0.3 and MySQL 5.7 innodb environment. The transaction isolation level is the default REPEATABLE-READ



## Origin

Pandas official (up to 1.0.5 version) `to_sql` method does not implement upsert feature. Its parameter `if_exist` has avaliable values as below:

> **if_exists: {‘fail’, ‘replace’, ‘append’}, default ‘fail’**
>
> How to behave if the table already exists.
>
> - fail: Raise a ValueError.
>
> - replace: Drop the table before inserting new values.
>
> - append: Insert new values to the existing table.

Notice that the **replace** here takes effect on the whole **table** rather than each specific row, which means the native method can only truncate the whole table and re-insert the entire DataFrame.

Demand for the upsert feature has been discussed in the official repo long before, see [issue #14553](https://github.com/pandas-dev/pandas/issues/14553). But the discussion has last for almost 4 years (up to 1.0.5 version), and the officials still can not deliver this feature in a stable release. For now it is only called "**may be fixed** by [#29636](https://github.com/pandas-dev/pandas/pull/29636)"

In those discussion, the major consideration of the officials are:

- Consistent support for multiple databases, but different databases can have huge difference on their native upsert pratice
- Also due to the disparity of databases, they would only support upsert by primary key in the first plan, not by unique constraints
- The conciseness of API

Therefore it can be expected that, the officials may be conservative on their first implementation for the balance of simplicity ("to protect users that have a poorly designed database") and function abundance: only support upsert by primary key for example. However, since the auto-increment has to be (part of) the primary key in MySQL, you won't be able to distinguish those duplicate records which need to be updated once you define an auto-increment. One possible solution is that the primary key contains only  an auto-increment, other columns to decide a unique record serve as the unique constraint. For this goal, this repo gives an unofficial solution only for MySQL, and it is highly probably not a best pratice.



Up to the last commit of this repo, pandas 1.0.5 version's native `to_sql` method still does not support **upsert**.



## Reference

- [pangres](https://github.com/ThibTrip/pangres): support multiple databases; upsert by primary key, require to set the primary key as the DataFrame index (**In fact it is completely compatible with unique constraint**); implement without temporary table, much faster.
- [pandabase](https://github.com/notsambeck/pandabase): support multiple databases; upsert by primary key, require to set the primary key as the DataFrame index; seems not supporting MySQL
- [pandas-to-mysql](https://github.com/frank690/pandas-to-mysql)
- [pandas-sql](https://github.com/xbanke/pandas-sql)
- [Pandas-to_sql-upsert](https://github.com/ryanbaumann/Pandas-to_sql-upsert)
