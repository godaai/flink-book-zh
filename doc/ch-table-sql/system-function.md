(system-function)=
# 系统函数

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::


## 系统内置函数概述

函数（Function）是 Flink Table API & SQL 中用于封装复杂逻辑、扩展数据处理能力的核心机制。它们允许用户在查询中执行各种计算和转换操作。Flink 中的函数可以从两个主要维度进行分类，这有助于理解它们的特性和使用范围。

第一个维度是根据函数的来源和注册方式，将其区分为**系统内置函数（System Functions）**和**目录函数（Catalog Functions）**。系统内置函数由 Flink 框架直接提供，无需额外注册即可在任何 SQL 查询或 Table API 调用中使用。这些函数覆盖了广泛的常见操作，例如数学计算、字符串处理、日期时间操作以及聚合等。正如在 `SystemFunctionExample.java` 示例中所见，像 `ARRAY[...]` 这样的构造函数用于创建数组，以及 `CARDINALITY(...)` 用于计算数组元素个数的函数，都属于系统内置函数的范畴，可以直接在 SQL 语句中使用。相对地，目录函数则是用户自定义或第三方提供的函数，它们被注册到特定的 Catalog 和 Database 下，调用时通常需要通过完整的命名空间路径（如 `mycatalog.mydb.myfunc`）来引用。

第二个维度是根据函数的生命周期，将其区分为**临时函数（Temporary Functions）**和**持久化函数（Persistent Functions）**。临时函数仅在当前的 Flink 会话（Session）内有效，一旦会话结束，函数定义便会丢失。而持久化函数则具有更长的生命周期，可以在多个会话之间共享。系统内置函数本质上都是持久化的。目录函数可以是临时的，也可以是持久化的，这取决于其注册方式。

综合这两个维度，函数可以被归入四种类别：临时系统函数（较少见）、系统函数、临时目录函数和（持久化）目录函数。本章将重点介绍最为常用且基础的**系统内置函数**，它们构成了 Flink SQL 功能的核心。后续章节将探讨如何创建和使用目录函数。系统内置函数种类繁多，本章将选取其中具有代表性的类别和函数进行讲解，并辅以示例。关于所有系统函数的完整列表和详细用法，建议查阅 Flink 官方文档。

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