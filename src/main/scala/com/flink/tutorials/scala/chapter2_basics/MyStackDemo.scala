package com.flink.tutorials.scala.chapter2_basics

object MyStackDemo {

  // Stack泛型类
  class Stack[T] {
   private var elements: List[T] = Nil
   def push(x: T) { elements = x :: elements }
   def peek: T = elements.head
  }

  // 泛型方法，检查两个Stack顶部是否相同
  def isStackPeekEquals[T](p: Stack[T], q: Stack[T]): Boolean = {
   p.peek == q.peek
  }

  def main(args: Array[String]): Unit = {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    println(stack.peek)

    val stack2 = new Stack[Int]
    stack2.push(2)
    val stack3 = new Stack[Int]
    stack3.push(3)
    println(isStackPeekEquals(stack, stack2))
    println(isStackPeekEquals(stack, stack3))
  }
}
