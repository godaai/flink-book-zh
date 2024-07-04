(generics)=
# 泛型

泛型（Generic）是强类型编程语言中经常使用的一种技术。很多框架的代码中都会大量使用泛型，比如在 Java 中我们经常看到如下的代码。

```java
List<String> strList = new ArrayList<String>();
List<Double> doubleList = new LinkedList<Double>();
```

在这段代码中，`ArrayList` 是一个泛型类，`List` 是一个泛型接口，它们提供给开发者一个放置不同类型的集合容器，我们可以向这个集合容器中添加 `String`、`Double` 以及其他各类数据类型。无论内部存储的是什么类型，集合容器提供给开发者的功能都是相同的，比如 `add()`，`get()` 等方法。有了泛型，我们就没必要创建 `StringArrayList`、`DoubleArrayList` 等类了，否则代码量太大，维护起来成本极高。

## Java 中的泛型

在 Java 中，泛型一般有 3 种使用方式：泛型类、泛型接口和泛型方法。一般使用尖括号 `<>` 来接收泛型参数。

### Java 泛型类

如代码清单 2-3 所示，我们定义一个泛型类 `MyArrayList`，这个类可以简单支持初始化和数据写入。只要在类名后面加上 `<T>` 就可以让这个类支持泛型，类内部的一些属性和方法都可以使用泛型 `T`。或者说，类的泛型会作用到整个类。

```java
public class MyArrayList<T> {

    private int size;
    T[] elements;

    public MyArrayList(int capacity) {
        this.size = capacity;
        this.elements = (T[]) new Object[capacity];
    }

    public void set(T element, int position) {
        elements[position] = element;
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = 0; i < size; i++) {
            result += elements[i].toString();
        }
        return result;
    }

    public static void main(String[] args){
        MyArrayList<String> strList = new MyArrayList<String>(2);
        strList.set("first", 0);
        strList.set("second", 1);

        System.out.println(strList.toString());
    }
}
```

代码清单 2-3 一个名为 `MyArrayList` 的泛型类，它可以支持简单的数据写入

当然我们也可以给这个类添加多个泛型参数，比如 `<K,V>`, `<T,E,K>` 等。泛型一般使用大写字母表示，Java 为此提供了一些大写字母使用规范，如下。

- `T` 代表一般的任何类。
- `E` 代表元素（Element）或异常（Exception）。
- `K` 或 `KEY` 代表键（Key）。
- `V` 代表值（Value），通常与 `K` 一起配合使用。

我们也可以从父类中继承并扩展泛型，比如 Flink 源码中有这样一个类定义，子类继承了父类的 `T`，同时自己增加了泛型 `K`：

```java
public class KeyedStream<T, K> extends DataStream<T> {
  ...
}
```

### Java 泛型接口

Java 泛型接口的定义和 Java 泛型类基本相同。下面的代码展示了在 `List` 接口中定义 `subList()` 方法，该方法对数据做截取。

```java
public interface List<E> {
    ...
    public List<E> subList(int fromIndex, int toIndex);
}
```

继承并实现这个接口的代码如下。

```java
public class ArrayList<E> implements List<E> {
    ...
    public List<E> subList(int fromIndex, int toIndex) {
        ...
        // 返回一个 List<E> 类型值
    }
}
```

这个例子中，要实现的 `ArrayList` 依然是泛型的。需要注意的是，`class ArrayList<E> implements List<E>` 这句声明中，`ArrayList` 和 `List` 后面都要加上 `<E>`，表明要实现的子类是泛型的。还有另外一种情况，要实现的子类不是泛型的，而是有确定类型的，如下面的代码。

```java
public class DoubleList implements List<Double> {
    ...
    public List<Double> subList(int fromIndex, int toIndex) {
        ...
        // 返回一个 List<Double> 类型值
    }
}
```

### Java 泛型方法

泛型方法可以存在于泛型类中，也可以存在于普通的类中。

```java
public class MyArrayList<T> {
    ...
    // public 关键字后的 <E> 表明该方法是一个泛型方法
    // 泛型方法中的类型 E 和泛型类中的类型 T 可以不一样
    public <E> E processElement(E element) {
        ...
        return E;
    }
}
```

从上面的代码可以看出，`public` 或 `private` 关键字后的 `<E>` 表示该方法一个泛型方法。泛型方法的类型 `E` 和泛型类中的类型 `T` 可以不一样。或者说，如果泛型方法是泛型类的一个成员，泛型方法既可以继续使用类的类型 `T`，也可以自己定义新的类型 `E`。

### 通配符

除了用 `<T>` 表示泛型，还可用 `<?>` 这种形式。`<?>` 被称为通配符，用来适应各种不同的泛型。此外，一些代码中还会涉及通配符的边界问题，主要是为了对泛型做一些安全性方面的限制。有兴趣的读者可以自行了解泛型的通配符和边界。

### 类型擦除

Java 的泛型有一个遗留问题，那就是类型擦除（Type Erasure）。我们先看一下下面的代码。

```java
Class<?> strListClass = new ArrayList<String>().getClass();
Class<?> intListClass = new ArrayList<Integer>().getClass();
// 输出：class java.util.ArrayList
System.out.println(strListClass);
// 输出：class java.util.ArrayList
System.out.println(intListClass);
// 输出：true
System.out.println(strListClass.equals(intListClass));
```

虽然声明时我们分别使用了 `String` 和 `Integer`，但运行时关于泛型的信息被擦除了，我们无法区别 `strListClass` 和 `intListClass` 这两个类型。这是因为，泛型信息只存在于代码编译阶段，当程序运行到 JVM 上时，与泛型相关的信息会被擦除。类型擦除对于绝大多数应用系统开发者来说影响不太大，但是对于一些框架开发者来说，必须要注意。比如，Spark 和 Flink 的开发者都使用了一些办法来解决类型擦除问题，对于 API 调用者来说，受到的影响不大。

## Scala 中的泛型

对 Java 的泛型有了基本了解后，我们接着来了解一下 Scala 中的泛型。相比而言，Scala 的类型系统更复杂，这里只介绍一些简单语法，使读者能够读懂一些源码。

Scala 中，泛型放在了方括号 `[]` 中。或者我们可以简单地理解为，原来 Java 的泛型类 `<T>`，现在改为 `[T]` 即可。

在代码清单 2-4 中，我们创建了一个名为 `Stack` 的泛型类，并实现了两个简单的方法，类中各成员和方法都可以使用泛型 `T`。我们也定义了一个泛型方法，形如 `isStackPeekEquals[T]()`，方法中可以使用泛型 `T`。

```scala
object MyStackDemo {

  // Stack 泛型类
  class Stack[T] {
   private var elements: List[T] = Nil
   def push(x: T) {elements = x :: elements}
   def peek: T = elements.head
  }

  // 泛型方法，检查两个 Stack 顶部是否相同
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
```

代码清单 2-4 使用 Scala 实现一个简易的 `Stack` 泛型类

## 泛型小结

本节简单总结了 Java 和 Scala 的泛型知识。对于初学者来说，泛型的语法有时候让人有些眼花缭乱，但其目的是接受不同的数据类型，增强代码的复用性。

泛型给开发者提供了不少便利，尤其是保证了底层代码简洁性。因为这些底层代码通常被封装为一个框架，会有各种各样的上层应用调用这些底层代码，进行特定的业务处理，每次调用都可能涉及泛型问题。包括 Spark 和 Flink 在内的很多框架都需要开发者基于泛型进行 API 调用。开发者非常有必要了解泛型的基本用法。
