(exercise-stock-basic)= 
# 习题 股票数据流处理

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

经过本章的学习，读者应该对 Flink 的 DataStream API 有了初步的认识，本节以股票价格这个场景来实践所学内容。

## 实验目的

针对具体的业务场景，学习如何定义相关数据结构，如何自定义 Source，如何使用各类算子。

## 实验内容

我们虚构了一个股票交易数据集，如下所示，该数据集中有每笔股票的交易时间、价格和交易量。数据集放置在了 `src/main/resource/stock` 文件夹里。

```
股票代号, 交易日期, 交易时间（秒）, 价格, 交易量
US2.AAPL,20200108,093003,297.260000000,100
US2.AAPL,20200108,093003,297.270000000,100
US2.AAPL,20200108,093003,297.310000000,100
```

在 [数据类型和序列化](./data-types.md) 章节，我们曾介绍 Flink 所支持的数据结构。对于这个股票价格的业务场景，首先要做的是对该业务进行建模，读者需要设计一个 `StockPrice` 类，能够表征一次交易数据。这个类至少包含以下字段：

```java
/* 
 * symbol      股票代号
 * ts          时间戳
 * price       价格
 * volume      交易量
 */
```

接下来，我们利用 File Source，读取数据集中的元素，并将数据写入 `DataStream<StockPrice>` 中。为了模拟不同交易之间的时间间隔，我们使用 `Thread.sleep()` 方法，等待一定的时间。

```java
// 利用 FileSource 读取数据集， filePath 是数据集的路径， StockReaderFormat 是自定义的 RecordStreamFormat
FileSource<StockPrice> source = FileSource
        .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
        .build();
// 不同交易之间的时间间隔为数据中的时间
DataStream<StockPrice> stockStream = env.fromSource(source, WatermarkStrategy.
        <StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.ts), "StockSource");
```
下面的代码展示了如何自定义 `StockReaderFormat` ，读者可以直接拿来借鉴。
```java
public class StockReaderFormat extends SimpleStreamFormat<StockPrice> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reader<StockPrice> createReader(Configuration config, FSDataInputStream stream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return new Reader<>() {
            @Override
            public StockPrice read() throws IOException {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }

                String[] fields = line.split(",");
                if (fields.length < 5) {
                    System.err.println("Invalid line: " + line + " (expected at least 5 fields)");
                    return read(); // 跳过无效行，继续读取下一行
                }

                try {
                    String symbol = fields[0];
                    String date = fields[1];
                    String time = fields[2];
                    double price = Double.parseDouble(fields[3]);
                    int volume = Integer.parseInt(fields[4]);

                    long ts = parseDateTime(date, time);
                    String mediaStatus = "";

                    return new StockPrice(symbol, price, ts, volume, mediaStatus);
                } catch (NumberFormatException | ParseException e) {
                    System.err.println("Failed to parse line: " + line + " - " + e.getMessage());
                    return read(); // 解析失败时继续读取下一行
                }
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    private long parseDateTime(String date, String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        Date dt = sdf.parse(date + " " + time);
        return dt.getTime();
    }

    @Override
    public TypeInformation<StockPrice> getProducedType() {
        return TypeInformation.of(StockPrice.class);
    }
}
```

对于我们自定义的这个 Source，我们可以使用下面的时间语义：

```java
WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy
    .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.ts);
```

这个WatermarkStrategy的含义是：
1. 使用`forBoundedOutOfOrderness`来处理乱序数据，允许最多5秒的乱序
2. 使用`withTimestampAssigner`来指定时间戳提取器，从StockPrice对象的ts字段获取事件时间

这样设置后，Flink就能正确处理事件时间语义，支持基于事件时间的窗口操作和时间相关计算。

接下来，基于 DataStream API，按照股票代号分组，对股票数据流进行分析和处理。

## 实验要求

完成数据结构定义和数据流处理部分的代码编写。其中数据流分析处理部分需要完成：

* 程序 1：价格最大值

实时计算某支股票的价格最大值。

* 程序 2：汇率转换

数据中股票价格以美元结算，假设美元和人民币的汇率为 7，使用 `map` 进行汇率转换，折算成人民币。

* 程序 3：大额交易过滤

数据集中有交易量字段，给定某个阈值，过滤出交易量大于该阈值的，生成一个大额交易数据流。

## 实验报告

将思路和程序撰写成实验报告。