(exercise-iot)=
# 练习：SQL on IoT

:::{note}

本教程已出版为《Flink原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

经过本章的学习，结合三到七章的背景知识，读者应该对Table API & SQL有了比较全面的了解，尤其SQL提供了一种更为通用的接口，相比Java/Scala来说更方便易用。本节将结合物联网（Internet of Things，IoT）场景来展示如何使用Flink SQL进行数据处理和分析。

## 实验目的

熟悉Flink SQL的各类操作：包括使用Flink SQL连接数据源，进行时间窗口操作，进行两个表的Temporal Table Join等。

## 实验内容

本实验基于德国纽伦堡大学提供的室内气候数据[^1]，该团队使用IoT设备来收集温度、湿度、光照等数据，结合室内人数、人物活动以及门窗开闭等情况，进行了研究，我们在原始数据基础上做了调整。

假设我们有多个房间（A、B、C等），每个房间内部署有传感器，传感器可以收集温度、相对湿度、光照等数据。下图展示了房间A的布局，其中A1至A4为传感器。


![室内数据收集示意图](./img/iot-room-a.png)

传感器收集上来的数据集名为`sensor.csv`，包括以下字段：`room,node_id,temp,humidity,light,ts`，各字段含义如下：

* `room`：房间号，这里为A、B、C等。
* `node_id`：该房间内的传感器编号，以上图中的房间A为例，共4个传感器，编号为1至4。
* `temp`：摄氏温度，单位°C。
* `humidity`：相对湿度，单位%。
* `light`：光传感器波长，单位nm。
* `ts`：数据收集时间戳，形如`2016-03-15 19:34:05`。

此外，第二个数据流收集房间的环境信息，数据集名为`env.csv`。这个数据记录了房间环境的变动，任何变动都将在数据中增加一条记录。数据包括以下字段：`room,occupant,activity,door,win,ts`，各字段含义如下：

* `room`：房间号，这里为A、B、C等。
* `occupant`：房间人数。
* `activity`：人物行为。0：无活动、1：读、2：站、3：行走、4：工作。
* `door`：门的状态。0：关、1：开。
* `win`：窗的状态，0：关、1：开。
* `ts`：以上环境信息发生改动的时间戳，形如`2016-03-15 19:34:05`。

两份数据集在工程目录的`src/main/resources/iot`下。

## 实验内容

结合本书内容和Flink官方文档，完成下面程序：

* 程序1：使用流处理模式，读取`sensor.csv`和`env.csv`数据源，注意`ts`字段作为时间属性，并定义Watermark策略。
* 程序2：按房间号分组，计算该房间内每分钟的平均温度和湿度，输出到文件系统上。
* 程序3：基于Temporal Table Join，将`sensor.csv`和`env.csv`按照房间号连接，生成一个宽表（宽表有助于哦后续数据分析流程），将结果输出到文件系统上。宽表包括以下字段：`room,node_id,temp,humidity,light,occupant,activity,door,win,ts`。

## 实验报告
将思路和程序撰写成实验报告。

[^1]: P. Morgner, C. Müller, M. Ring, B. Eskofier, C. Riess, F. Armknecht, Z. Benenson: Privacy Implications of Room Climate Data. In the Procedings of the European Symposium on Research in Computer Security (ESORICS) 2017, Oslo, Norway, September 11-13, 2017.