(functional-programming)=
# 函数式编程

函数式编程（Functional Programming）是一种编程范式，因其更适合做并行计算，近年来开始受到大数据开发者的广泛关注。Python、JavaScript等语言对函数式编程的支持都不错；Scala更是以函数式编程的优势在大数据领域“攻城略地”；即使是Java，也为了适应函数式编程，加强对函数式编程的支持。未来的程序员或多或少都要了解一些函数式编程思想。这里抛开一些数学推理等各类复杂的概念，仅从Flink开发的角度带领读者熟悉函数式编程。

## 2.3.1 函数式编程思想简介

在介绍函数式编程前，我们可以先回顾传统的编程范式如何解决一个数学问题。假设我们想求解一个数学表达式：

```plaintext
addResult = x + y
result = addResult * z
```

在这个例子中，我们要先求解中间结果，将其存储到中间变量，再进一步求得最终结果。这仅仅是一个简单的例子，在更多的编程实践中，程序员必须告诉计算机每一步执行什么命令、需要声明哪些中间变量等。因为计算机无法理解复杂的概念，只能听从程序员的指挥。

中学时代，我们的老师在数学课上曾花费大量时间讲解函数，函数指对于自变量的映射。函数式编程的思想正是基于数学中对函数的定义。其基本思想是，在使用计算机求解问题时，我们可以把整个计算过程定义为不同的函数。比如，将这个问题转化为：

```plaintext
result = multiply(add(x, y), z)
```

我们再对其做进一步的转换：

```plaintext
result = add(x, y).multiply(z)
```

传统思路中要创建中间变量，要分步执行，而函数式编程的形式与数学表达式形式更为相似。人们普遍认为，这种函数式的描述更接近人类自然语言。

如果要实现这样一个函数式程序，主要需要如下两步。
1. 实现单个函数，将零到多个输入转换成零到多个输出。比如`add()`这种带有映射关系的函数，它将两个输入转化为一个输出。
2. 将多个函数连接起来，实现所需业务逻辑。比如，将`add()`、`multiply()`连接到一起。

接下来我们通过Java代码来展示如何实践函数式编程思想。

## 2.3.2 Lambda表达式的内部结构

数理逻辑领域有一个名为λ演算的形式系统，主要研究如何使用函数来表达计算。一些编程语言将这个概念应用到自己的平台上，期望能实现函数式编程，取名为Lambda表达式（λ的英文拼写为Lambda）。

我们先看一下Java的Lambda表达式的语法规则。

```java
(parameters) -> {
  body 
}
```

Lambda表达式主要包括一个箭头符号`->`，其两边连接着输入参数和函数体。我们再看看代码清单 2-5中的几个Java Lambda表达式。

```java
// 1. 无参数，返回值为5  
() -> 5  

// 2. 接收1个int类型参数，将其乘以2，返回一个int类型值
x -> 2 * x

// 3. 接收2个int类型参数，返回它们的差
(x, y) -> x – y  

// 4. 接收2个int类型参数，返回它们的和
(int x, int y) -> x + y

// 5. 接收1个String类型参数，将其输出到控制台，不返回任何值
(String s) -> { System.out.print(s); }

// 6. 参数为圆半径，返回圆面积，返回值为double类型
(double r) -> {
    double pi = 3.1415;
    return r * r * pi;
}
```

代码清单 2-5 Java Lambda表达式

可以看到，这几个例子都有一个`->`，表示这是一个函数式的映射，相对比较灵活的是左侧的输入参数和右侧的函数体。图2-2所示为Java Lambda表达式的拆解，这很符合数学中对一个函数做映射的思维方式。

![图2-2  Java Lambda表达式拆解](./img/lambda.png)

## 2.3.3 函数式接口

通过前文的几个例子，我们大概知道Lambda表达式的内部结构了，那么Lambda表达式到底是什么类型呢？在Java中，Lambda表达式是有类型的，它是一种接口。确切地说，Lambda表达式实现了一个函数式接口（Functional Interface），或者说，前文提到的一些Lambda表达式都是函数式接口的具体实现。

函数式接口是一种接口，并且它只有一个虚函数。因为这种接口只有一个虚函数，因此其对应英文为Single Abstract Method（SAM）。SAM表示这个接口对外只提供这一个函数的功能。如果我们想自己设计一个函数式接口，我们应该给这个接口添加`@FunctionalInterface`注解。编译器会根据这个注解确保该接口是函数式接口，当我们尝试往该接口中添加超过一个虚函数时，编译器会报错。在代码清单 2-6中，我们设计一个加法的函数式接口`AddInterface`，然后实现这个接口。

```java
@FunctionalInterface
interface AddInterface<T> {
    T add(T a, T b);
}

public class FunctionalInterfaceExample {

    public static void main( String[] args ) {

        AddInterface<Integer> addInt = (Integer a, Integer b) -> a + b;
        AddInterface<Double> addDouble = (Double a, Double b) -> a + b;

        int intResult;
        double doubleResult;

        intResult = addInt.add(1, 2);
        doubleResult = addDouble.add(1.1d, 2.2d);
    }
}
```

代码清单 2-6 一个能够实现加法功能的函数式接口

Lambda表达式实际上是在实现函数式接口中的虚函数，Lambda表达式的输入类型和返回类型要与虚函数定义的类型相匹配。

假如没有Lambda表达式，我们仍然可以实现这个函数式接口，只不过代码比较“臃肿”。首先，我们需要声明一个类来实现这个接口，可以是下面的类。

```java
public static class MyAdd implements AddInterface<Double> {
    @Override
    public Double add(Double a, Double b) {
            return a + b;
    }
}
```

然后，在业务逻辑中这样调用：`doubleResult = new MyAdd().add(1.1d, 2.2d);`。或者是使用匿名类，省去`MyAdd`这个名字，直接实现`AddInterface`并调用：

```java
doubleResult = new AddInterface<Double>(){
    @Override
    public Double add(Double a, Double b) {
        return a + b;
    }
}.add(1d, 2d);
```

声明类并实现接口和使用匿名类这两种方法是Lambda表达式出现之前，Java开发者经常使用的两种方法。实际上我们想实现的逻辑仅仅是一个`a + b`，其他代码其实都是冗余的，都是为了给编译器看的，并不是为了给程序员看的。有了比较我们就会发现，Lambda表达式的简洁优雅的优势就凸显出来了。

为了方便大家使用，Java内置了一些的函数式接口，放在`java.util.function`包中，比如`Predicate`、`Function`等，开发者可以根据自己需求实现这些接口。这里简单展示一下这两个接口。

`Predicate`对输入进行判断，符合给定逻辑则返回`true`，否则返回`false`。

```java
@FunctionalInterface
public interface Predicate<T> {
    // 判断输入的真假，返回boolean类型值
    boolean test(T t);
}
```

`Function`接收一个类型`T`的输入，返回一个类型`R`的输出。

```java
@FunctionalInterface
public interface Function<T, R> {
     // 接收一个类型T的输入，返回一个类型R的输出
     R apply(T t);
}
```

部分底层代码提供了一些函数式接口供开发者调用，很多框架的API就是类似上面的函数式接口，开发者通过实现接口来完成自己的业务逻辑。Spark和Flink对外提供的Java API其实就是这种函数式接口。

## 2.3.4 Java Stream API

Stream API是Java 8 的一大亮点，它与`java.io`包里的`InputStream`和`OutputStream`是完全不同的概念，也不是Flink、Kafka等大数据流处理框架中的数据流。它专注于对集合（Collection）对象的操作，是借助Lambda表达式的一种应用。通过Java Stream API，我们可以体验到Lambda表达式带来的编程效率的提升。

我们看一个简单的例子，代码清单 2-7首先过滤出非空字符串，然后求得每个字符串的长度，最终返回为一个`List<Integer>`类型值。代码使用了Lambda表达式来完成对应的逻辑。

```java
List<String> strings = Arrays.asList(
  "abc", "", "bc", "12345",
  "efg", "abcd","", "jkl");

List<Integer> lengths = strings
  .stream()
  .filter(string -> !string.isEmpty())
  .map(s -> s.length())
  .collect(Collectors.toList());

lengths.forEach((s) -> System.out.println(s));
```

代码清单 2-7 使用Lambda表达式来完成对String类型列表的操作

这段代码中，数据先经过`stream()`方法被转换为一个`Stream`类型，后经过`filter()`、`map()`、`collect()`等处理逻辑，生成我们所需的输出。各个操作之间使用英文点号`.`来连接，这种方式被称作方法链（Method Chaining）或者链式调用。链式调用可以被抽象成一个管道（Pipeline），将代码清单2-7进行抽象，可以形成图2-3所示的Stream管道。

![图2-3  Stream管道](./img/stream.png)

## 2.3.5 函数式编程小结

函数式编程更符合数学上函数映射的思想。具体到编程语言层面，我们可以使用Lambda表达式来快速编写函数映射，函数之间通过链式调用连接到一起，完成所需业务逻辑。Java的Lambda表达式是后来才引入的，而Scala天生就是为函数式编程所设计。由于在并行处理方面的优势，函数式编程正在被大量应用于大数据处理领域。

对Lambda表达式、Java Stream API以及Flink API有了基本了解后，我们也应该注意不要将Java Stream API与Flink API混淆。
