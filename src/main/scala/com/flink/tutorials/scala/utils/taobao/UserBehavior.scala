package com.flink.tutorials.scala.utils.taobao

/**
  * 用户行为
  * categoryId为商品类目ID
  * behavior包括点击（pv）、购买（buy）、加购物车（cart）、喜欢（fav）
  * */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)
