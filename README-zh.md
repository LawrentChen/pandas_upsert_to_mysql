**注意：强烈建议使用 [pangres](https://github.com/ThibTrip/pangres)，其功能实现远胜于本模块**



# pandas_upsert_to_mysql

pandas DataFrame 增强的  `to_sql`  方法。仅针对写入 MySQL 数据库，借助临时表提供相对便捷的  **upsert** (insert or update)  功能实现

- 支持根据主键/唯一约束判断行记录是否需要进行更新
- 要求 MySQL 数据库表结构必须设计良好（需要配合使用 SQLAlchemy）
- 要求 MySQL 自增列作为单独主键。不允许自增列与其他列组成复合主键



## 安装

```shell
pip install pandas_upsert_to_mysql
```



## 用法示例 Usage and Example

以一张订单表为例子，这里 `row_id` 为自增主键，`order_id` 和 `product_id` 构成唯一约束（一张订单内可以有多种不同货品）

```mysql
-- 这里为了便于说明使用原生 SQL 建表，实际需要配合使用 SQLAlchemy

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

该表格随着时间推移需要进行以下两种操作

1. 新订单：插入新纪录
2. 原有订单发生退款：更新原纪录中的 `refund_qty` (退款数量)字段

我们先插入初始数据，这一步可以简单地用 pandas 官方原生的 `to_sql` 轻松实现

| row_id | order_id | product_id |  qty | refund_qty | update_time         | create_time         |
| -----: | :------- | :--------- | ---: | ---------: | :------------------ | :------------------ |
|      1 | A0001    | PD100      |   10 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |
|      2 | A0002    | PD200      |   20 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |
|      3 | A0002    | PD201      |   22 |          0 | 2020-06-26 11:11:55 | 2020-06-26 11:11:55 |

假设数据库中已经有了上表数据，现在需要进行 upsert 的新数据如下。注意 A0002-PD201 如今有了 2 件退款，并且新增了 A0003-PD300 记录

| order_id | product_id |  qty | refund_qty |
| :------- | :--------- | ---: | ---------: |
| A0001    | PD100      |   10 |          0 |
| A0002    | PD200      |   20 |          0 |
| A0002    | PD201      |   22 |          2 |
| A0003    | PD300      |   30 |          0 |



```python
# table 是包含 SQLAlchemy ORM 定义的表结构类的模块
# engine 则是一个需要你自己定义的 sqlalchemy.engine.Engine，用于连接到目标 MySQL 数据库，与 pandas.DataFrame.to_sql 中的 con 参数要求相同
# 此处的 table 和 engine 仅为模块内示例，实际使用时应自己另行定义

import pandas_upsert_to_mysql.table as table
from pandas_upsert_to_mysql import Upsert

Upsert(engine=engine).to_mysql(df=table.ExampleOrderTable.new_df,
                               target_table=table.Order,
                               temp_table=table.OrderTemp,
                               if_record_exists='update')
```

结果如下，注意原有的三条记录中只有 row_id = 3 的记录 `update_time` 发生了变化，完全符合预期

| row_id | order_id | product_id |  qty | refund_qty | update_time             | create_time             |
| -----: | :------- | :--------- | ---: | ---------: | :---------------------- | :---------------------- |
|      1 | A0001    | PD100      |   10 |          0 | 2020-06-26 11:11:55     | 2020-06-26 11:11:55     |
|      2 | A0002    | PD200      |   20 |          0 | 2020-06-26 11:11:55     | 2020-06-26 11:11:55     |
|      3 | A0002    | PD201      |   22 |          2 | **2020-06-26 11:13:19** | 2020-06-26 11:11:55     |
|      4 | A0003    | PD300      |   30 |          0 | **2020-06-26 11:13:19** | **2020-06-26 11:13:19** |



## 警告Caveats

- 唯一约束中任意一列均不能有 null 空值，否则将违背约束造成记录重复。这被认为是 MySQL 的一个历史久远的 bug [#8173](https://bugs.mysql.com/bug.php?id=8173)，这意味着 DataFrame 在写入数据库前，必须为唯一约束中的每一列指定一个默认值，替换其可能出现的 null 空值
- 使用了 SQLAlchemy 的 `session scope`，但鉴于开发者不是专业工程师，还是不能保证并发情形的可靠性
- 仅在 Pandas >= 1.0.3 与 MySQL 5.7 Innodb 环境下测试，事务隔离等级为默认的可重复读



## 源起

pandas 官方（截至 1.0.5 版本）提供的 `to_sql` 方法并未实现 upsert 功能。其 `if_exist` 可选参数功能如下：

> **if_exists: {‘fail’, ‘replace’, ‘append’}, default ‘fail’**
>
> How to behave if the table already exists.
>
> - fail: Raise a ValueError.
>
> - replace: Drop the table before inserting new values.
>
> - append: Insert new values to the existing table.

注意此参数的 **replace** 是对**全表**而非具体某一行记录生效，也就是说原生方法只能将整张表完全删除后，再全部重新插入

关于逐行记录 upsert 需求早已有开发者于官方 repo 展开讨论，详见 [issue #14553](https://github.com/pandas-dev/pandas/issues/14553)。然而讨论持续了将近 4 年时间（截至 1.0.5 版本），官方仍未在正式版本中提供此功能，暂时仅称“**may be fixed** by [#29636”](https://github.com/pandas-dev/pandas/pull/29636)

从讨论中可以看到官方的主要考量：

- 对多个不同的数据库实现统一支持，然而各个数据库关于 upsert 的原生实现又有很大不同
- 同样是由于数据库之间的差异，初步仅支持根据主键进行的 upsert，无法基于唯一约束进行处理
- API 参数的简洁性和语义上的明确性

因此可以预期，官方为了平衡简洁性（“为了保护那些使用设计糟糕的数据库的用户”）和功能丰富性，初步实现将会倾向于保守：比如仅支持通过主键判断对记录进行更新。**然而 MySQL 中的自增字段必须设为主键（的一部分）**，因此一旦纳入自增字段则无法再辨别出需要更新的重复记录。一个解决办法是主键仅包含自增字段，剩余唯一记录标识作为表的唯一约束。为此，本  repo 仅针对 MySQL 给出非官方的实现，很有可能并不是最佳实践。



截至本 repo 最后一次 commit，pandas 1.0.5 版本原生的 `to_sql` 方法仍未支持 **upsert**



## API 文档





## 参考

- [pangres](https://github.com/ThibTrip/pangres)：支持多种数据库实现；通过主键进行 upsert，要求 dataframe index 为主键（**实质上兼容唯一约束**）；不使用临时表，速度更快；实验证明可以兼容事先建立起唯一约束的表模式
- [pandabase](https://github.com/notsambeck/pandabase)：支持多种数据库实现；通过主键进行 upsert，要求 dataframe index 为主键；似乎不支持 MySQL
- [pandas-to-mysql](https://github.com/frank690/pandas-to-mysql)
- [pandas-sql](https://github.com/xbanke/pandas-sql)
- Pandas-to_sql-upsert(https://github.com/ryanbaumann/Pandas-to_sql-upsert)
