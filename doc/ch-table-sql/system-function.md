(system-function)=
# 系统函数

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::


## 函数简介

Table API & SQL 提供给用户强大的操作数据的方法：函数（Function）。对于 Function，可以有两种维度来对其分类。

第一种维度根据是否为系统内置（System）来分类。System Function 是 Flink 提供的内置函数，在任何地方都可以直接拿来使用。非系统内置函数一般注册到一个 Catalog 下的 Database 里，该函数有自己的命名空间（Namespace），表示该函数归属于哪个 Catalog 和 Database。例如，使用名为 `func` 的函数时需要加上 Namespace 前缀：`mycatalog.mydb.func`。由于函数被注册到 Catalog 中，这种函数被称为表目录函数（Catalog Function）。

第二种维度根据是否为临时函数来分类。临时函数（Temporary Function）只存在于一个 Flink Session 中，Session 结束后就被销毁，其他 Session 无法使用。非临时函数，又被成为持久化函数（Persistent Function），可以存在于多个 Flink Session 中，它可以是一个 System Function，也可以是一个 Catalog Function。

根据这两个维度，Function 可以被划分四类：

* Temporary System Function
* System Function
* Temporary Catalog Function
* Catalog Function

这些函数可以在 Table API 中使用 Java、Scala 或 Python 语言调用，也可以在 Flink SQL 中以 SQL 语句的形式调用。这里以 SQL 为例来介绍如何使用这些函数。绝大多数 System Function 已经内置在 Table API & SQL 中，它们也是 Persistent Function，本节将主要介绍这部分内容，下节将介绍 Catalog Function。由于 System Function 较多，这里只介绍一些常用的函数并提供一些例子，其他函数的具体使用方法可以参考 Flink 的官方文档。

## 标量函数

标量函数（Scalar Function）接收零个、一个或者多个输入，生成一个单值输出。

### 比较函数

* `value1 = value2`

如果 `value1` 和 `value2` 相等，返回 `TRUE`；如果 `value1` 或 `value2` 任何一个值为 `NULL`，返回 `UNKNOWN`。

* `value1 <> value2`

如果 `value1` 和 `value2` 不相等，返回 `TRUE`；如果 `value1` 或 `value2` 任何一个值为 `NULL`，返回 `UNKNOWN`。
* `value1 >= value2`

如果 `value1` 大于等于 `value2`，返回 `TRUE`；如果 `value1` 或 `value2` 任何一个值为 `NULL`，返回 `UNKNOWN`。其他 `>`、`<`、<`=` 比较函数与此相似。
* `value IS NULL` 和 `value IS NOT NULL`

判断 `value` 是否为 `NULL`。
* `value1 BETWEEN [ASYMMETRIC | SYMMETRIC] value2 AND value3`

判断 `value1` 是否在一个区间。支持 `DOUBLE`、`BIGINT`、`INT`、`VARCHAR`、`DATE`、`TIMESTAMP`、`TIME` 这些类型。

例如，`12 BETWEEN 15 AND 12` 返回 `FALSE`，`12 BETWEEN SYMMETRIC 15 AND 12` returns `TRUE`。`SYMMETRIC` 表示包含区间边界。`value1 NOT BETWEEN [ASYMMETRIC | SYMMETRIC] value2 AND value3` 与之相似。

* `string1 LIKE string2`

如果 `string1` 符合 `string2` 的模板，返回 `TRUE`。`LIKE` 主要用于字符串匹配，`string2` 中可以使用 `%` 来定义通配符。例如，`'TEST' LIKE '%EST'` 返回 `TRUE`。`string1 NOT LIKE string2` 与之类似。

* `string1 SIMILAR TO string2`

如果 `string1` 符合 SQL 正则表达式 `string2`，返回 `TRUE`。例如，`'TEST' SIMILAR TO '.EST'` 返回 `TRUE`。`string1 NOT SIMILAR TO string2` 与之类似。

* `value1 IN (value2 [, value3]* )`：如果 `value1` 在列表中，列表包括 `value2`、`value3` 等元素，返回 `TRUE`。例如，`'TEST' IN ('west', 'TEST', 'rest')` 返回 `TRUE`；`'TEST' IN ('west', 'rest')` 返回 `FALSE`。`value1 NOT IN (value2 [, value3]* )` 与之类似。

* `EXISTS (sub-query)`：如果子查询有至少一行结果，返回 `TRUE`。例如，下面的 SQL 语句使用了 `EXISTS`，实际上起到了 Join 的作用：

```sql
SELECT * 
FROM l 
WHERE EXISTS (select * from r where l.a = r.c)
```
	
* `value IN (sub-query)`：如果 `value` 等于子查询中的一行结果，返回 `TRUE`。`value NOT IN (sub-query)` 与之类似。例如：

```sql
SELECT * 
FROM tab 
WHERE a IN (SELECT c FROM r)
```

:::note
在流处理模式下，`EXISTS(sub-query)` 和 `value IN (sub-query)` 都需要使用状态进行计算，我们必须确保配置了状态过期时间，否则状态可能会无限增大。
:::

### 逻辑函数

* `boolean1 OR boolean2`

如果 `boolean1` 或 `boolean2` 任何一个为 `TRUE`，返回 `TRUE`。

* `boolean1 AND boolean2`

如果 `boolean1` 和 `boolean2` 都为 `TRUE`，返回 `TRUE`。

* `NOT boolean`

如果 `boolean` 为 `TRUE`，返回 `FALSE`；`boolean` 为 `FALSE`，返回 `TRUE`。

* `boolean IS FALSE`、`boolean IS TRUE` 和 `boolean IS UNKNOWN`

根据 `boolean` 结果，判断是否为 `FALSE`、`TRUE` 或者 `UNKNOWN`。`boolean IS NOT FALSE` 等与之类似。

### 数学函数

* 加减乘除

加（`+`）减（`-`）乘（`*`）除（`/`）对数字字段做运算。下面的例子以加法为例，其他运算与之类似。

```sql
SELECT int1+int2 AS add
FROM tab 
```

* `ABS(numeric)`

返回 `numeric` 的绝对值。

* `MOD(numeric1, numeric2)`

余数函数，`numeric1` 除以 `numeric2`，返回余数。

* `SQRT(numeric)`

平方根函数，返回 `numeric` 的平方根。

* `LN(numeric)`、`LOG10(numeric)` 和 `LOG2(numeric)`


对数函数，返回 `numeric` 的对数，分别以 e 为底、以 10 为底和以 2 为底。

* `EXP(numeric)`

指数函数，返回以 e 为底 `numeric` 的指数。

* `SIN(numeric)`、`COS(numeric)` 等

三角函数，包括 `SIN`、`COS`、`TAN` 等。

* `RAND()`

返回 0 到 1 之间的一个伪随机数。

* `CEIL(numeric)` 和 `FLOOR(numeric)`

向上和向下取整。

### 字符串函数

* `string1 || string2`

连接两个字符串。

* `CONCAT(string1, string2,...)`

连接多个字符串。

* `CHAR_LENGTH(string)` 和 `CHARACTER_LENGTH(string)`

返回字符串 `string` 的长度。

* `SUBSTRING(string FROM start [ FOR length])`

对字符串做截断，返回 `string` 的一部分，从 `start` 位置开始，默认到字符串结尾结束，填写 `length` 参数后，字符串截断到 `length` 长度。

* `POSITION(string1 IN string2)`

返回 `string1` 在 `string2` 中第一次出现的位置，如果未曾出现则返回 0。

* `TRIM([BOTH | LEADING | TRAILING] string1 FROM string2)`

将 `string2` 中出现的 `string1` 移除。`BOTH` 选项表示移除左右两侧的字符串。一般情况下，如果不指定 `string1`，默认移除空格。例如，`TRIM(LEADING 'x' FROM 'xxxxSTRINGxxxx')` 返回 `STRINGxxxx`。

* `REGEXP_REPLACE(string1, string2, string3)`

替换函数，将 `string1` 中符合正则表达式 `string2` 的字符全替换为 `string3`。例如，`REGEXP_REPLACE('foobar', 'oo|ar', '')` 移除了正则表达式 `oo|ar`，返回 `fb`。

### 时间函数

* `DATE string`、`TIME string` 和 `TIMESTAMP string` 

将字符串 `string` 转换为 `java.sql.Date`、`java.sql.Time` 或 `java.sql.Timestamp`。我们可以在 `WHERE` 语句中做过滤：

```sql
SELECT * FROM tab
WHERE b = DATE '1984-07-12' 
AND c = TIME '14:34:24' 
AND d = TIMESTAMP '1984-07-12 14:34:24
```

或者应用在 `SELECT` 语句中：

```sql
SELECT a, b, c,
 DATE '1984-07-12',
 TIME '14:34:24',
 TIMESTAMP '1984-07-12 14:34:24'
FROM tab
```

* `LOCALTIME`、`LOCALTIMESTAMP`

返回当前本地时间，格式为 `java.sql.Time` 和 `java.sql.Timestamp`。

* `YEAR(date)`、`MONTH(date)` 和 `DAYOFWEEK(date)` 等

将 `java.sql.Date` 转化为年月日。例如，`YEAR(DATE '1994-09-27')` 返回为 1994，`MONTH(DATE '1994-09-27')` 返回为 9，`DAYOFYEAR(DATE '1994-09-27')` 返回为 270。

* `HOUR(timestamp)`、`MINUTE(timestamp)` 和 `SECOND(timestamp)`

将 `java.sql.Timestamp` 转化为时分秒。例如，`HOUR(TIMESTAMP '1994-09-27 13:14:15')` 返回 13，`MINUTE(TIMESTAMP '1994-09-27 13:14:15')` 返回 14。

* `FLOOR(timepoint TO timeintervalunit)` 和 `CEIL(timepoint TO timeintervalunit)`

向下和向上取整。例如，`FLOOR(TIME '12:44:31' TO MINUTE)` 返回 12:44:00，`CEIL(TIME '12:44:31' TO MINUTE)` 返回 12:45:00。

### 判断函数

* `CASE ... WHEN ... END`

类似很多编程语言提供的 `switch ... case ...` 判断逻辑。在 Flink SQL 中可以对某个字段进行判断，其模板为：
```sql
CASE value
	WHEN value1_1 [, value1_2]* THEN result1
	[WHEN value2_1 [, value2_2]* THEN result2 ]*
	[ELSE resultZ]
END
```
例如，对表中字段 `a` 进行判断，生成一个新字段 `correct`，SQL 语句可以写为：

```sql
SELECT 
  CASE a
    WHEN 1 THEN 1
    ELSE 99
  END AS correct
FROM tab
```

也可以对一个表达式进行判断，其模板为：

```sql
CASE
	WHEN condition1 THEN result1
	[WHEN condition2 THEN result2]*
	[ELSE resultZ]
END
```

例如，对表中字段 `c` 进行 `c > 0` 的判断，为 `TRUE` 时生成 `b`，SQL 语句可以写为：

```sql
SELECT 
	CASE 
		WHEN c > 0 THEN b 
		ELSE NULL 
	END 
FROM tab
```

### 类型转化

* `CAST(value AS type)`

将字段 `value` 转化为类型 `type`。例如，`int1` 字段原本为 `INT`，现将其转化为 `DOUBLE`：

```sql
SELECT CAST(int1 AS DOUBLE) as aa
FROM tab
```

### 集合函数

* `ARRAY ‘[’ value1 [, value2]* ‘]’`

将多个字段连接成一个列表。例如，某表中两个字段 `a` 和 `b` 均为 `INT` 类型，将其连接到一起，后面再添加一个数字 99：

```sql
SELECT 
	ARRAY[a, b, 99] 
FROM tab
```

* `CARDINALITY(array)`

返回列表中元素的个数。例如前面这个例子中：

```sql
SELECT CARDINALITY(arr) 
FROM (
  SELECT ARRAY[a, b, 99] AS arr FROM tab
)
```

`ARRAY[a, b, 99]` 创建一个 3 个字段组成的列表，`CARDINALITY(arr) ` 返回值为 3。

## 聚合函数

在 [窗口](sql-window) 部分我们重点讲解了 `GROUP BY` 和 `OVER WINDOW` 的窗口划分方式，聚合函数一般应用在窗口上，对窗口内的多行数据进行处理，并生成一个聚合后的结果。

* `COUNT([ALL] expression | DISTINCT expression1 [, expression2]*)`

返回行数，默认情况下是开启了 `ALL` 选项，即返回所有行。使用 `DISTINCT` 选项后，对数据做去重处理。

* `AVG([ALL | DISTINCT] expression)`

返回平均值，默认情况下开启了 `ALL` 选项。使用 `DISTINCT` 选项后，对数据做去重处理。

* `SUM([ALL | DISTINCT] expression)`

对数据求和，默认情况下开启了 `ALL` 选项。使用 `DISTINCT` 选项后，对数据做去重处理。

* `MAX([ALL | DISTINCT] expression)` 和 `MIN([ ALL | DISTINCT] expression)`

求数据中的最大值 / 最小值，默认情况下开启了 `ALL` 选项。使用 `DISTINCT` 选项后，对数据做去重处理。

* `STDDEV_POP([ALL | DISTINCT] expression)`

求数据总体的标准差，默认情况下开启了 `ALL` 选项。使用 `DISTINCT` 选项后，对数据做去重处理。

## 时间单位

一些时间相关计算需要使用时间单位，常见的有 `YEAR`、`MONTH`、`WEEK`、`DAY`、`HOUR`、`MINUTE` 和 `SECOND` 等。