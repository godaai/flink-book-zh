package com.flink.tutorials.scala.utils.stock

/**
  * case class StockPrice
  * symbol      股票代号
  * ts          时间戳
  * price       价格
  * volume      交易量
  * mediaStatus 媒体对该股票的评价状态
  * */
case class StockPrice(symbol: String = "",
                      price: Double = 0d,
                      ts: Long = 0,
                      volume: Int = 0,
                      mediaStatus: String = "")
