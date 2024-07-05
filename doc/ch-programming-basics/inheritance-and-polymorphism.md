(inheritance-and-polymorphism)=
# 继承和多态

继承和多态是现代编程语言中最为重要的概念。继承和多态允许用户将一些代码进行抽象，以达到复用的目的。Flink 开发过程中会涉及大量的继承和多态相关问题。

## 继承、类和接口

继承在现实世界中无处不在。比如我们想描述动物和它们的行为，可以先创建一个动物类别，动物类别又可以分为狗和鱼，这样的一种层次结构其实就是编程语言中的继承关系。动物类涵盖了每种动物都有的属性，比如名字、描述信息等。从动物类衍生出的众多子类，比如鱼类、狗类等都具备动物的基本属性。不同类型的动物又有自己的特点，比如鱼会游泳、狗会吼叫。继承关系保证所有动物都具有动物的基本属性，这样就不必在创建一个新的子类的时候，将它们的基本属性（名字、描述信息）再复制一遍。同时，子类更加关注自己区别于其他类的特点，比如鱼所特有的游泳动作。

{numref}`fig-extend` 所示为对动物进行的简单的建模。其中，每个动物都有一些基本属性，即名字（name）和描述（description）；有一些基本方法，即 getName()和 eat()，这些基本功能共同组成了 Animal 类。在 Animal 类的基础上，可以衍生出各种各样的子类、子类的子类等。比如，Dog 类有自己的 dogData 属性和 bark()方法，同时也可以使用父类的 name 等属性和 eat() 方法。

我们将 {numref}`fig-extend` 所示的 Animal 类继承关系转化为代码，一个 Animal 公共父类可以抽象如代码清单 2-1 所示。

```java
public class Animal { 

    private String name;
    private String description;  

    public Animal(String myName, String myDescription) { 
        this.name = myName; 
        this.description = myDescription;
    } 

    public String getName() {
      return this.name;
    }

    public void eat(){ 
        System.out.println(name + "正在吃"); 
    }
}
```

代码清单 2-1 一个简单的 Animal 类

```{figure} ./img/extend.png
---
name: fig-extend
width: 60%
---
Animal 类继承关系
```

子类可以拥有父类非 private 的属性和方法，同时可以扩展属于自己的属性和方法。比如 Dog 类或 Fish 类可以继承 Animal 类，可以直接复用 Animal 类里定义的属性和方法。这样就不存在代码的重复问题，整个工程的可维护性更好。在 Java 和 Scala 中，子类继承父类时都要使用 extends 关键字。代码清单 2-2 实现了一个 Dog 类，并在里面添加了 Dog 类的一些特有成员。

```java
public class Dog extends Animal implements Move { 

    private String dogData;  

    public Dog(String myName, String myDescription, String myDogData) { 
        this.name = myName; 
        this.description = myDescription;
        this.dogData = myDogData;
    }

    @Override
    public void move(){ 
        System.out.println(name + "正在奔跑"); 
    }

    public void bark(){ 
        System.out.println(name + "正在叫"); 
    }
}
```

代码清单 2-2 Dog 类继承 Animal 类，并实现了一些特有的成员

不过，Java 只允许子类继承一个父类，或者说 Java 不支持多继承。`class A extends B, C` 这样的语法在 Java 中是不允许的。另外，有一些方法具有更普遍的意义，比如 move() 方法，不仅动物会移动，一些机器（比如 Machine 类和 Car 类）也会移动。因此让 Animal 类和 Machine 类都继承一个 Mover 类在逻辑上没有太大意义。对于这种场景，Java 提供了接口，以关键字 `interface` 标注，可以将一些方法进一步抽象出来，对外提供一种功能。不同的子类可以继承相同的接口，实现自己的业务逻辑，也解决了 Java 不允许多继承的问题。代码清单 2-2 的 Dog 类也实现了这样一个名为 Move 的接口。

Move 接口的定义如下。

```java
public interface Move {
    public void move();
}
```

:::{note}

在 Java 中，一个类可以实现多个接口，并使用 `implements` 关键字。

```java
class ClassA implements Move, InterfaceA, InterfaceB {
  ...
}
```

在 Scala 中，一个类实现第一个接口时使用关键字 `extends`，后面则使用关键字 `with`。

```scala
class ClassA extends Move with InterfaceA, InterfaceB {
  ...
}
```

接口与类的主要区别在于，从功能上来说，接口强调特定功能，类强调所属关系；从技术实现上来说，接口里提供的都是抽象方法，类中只有用 `abstract` 关键字定义的方法才是抽象方法。抽象方法是指只定义了方法签名，没有定义具体实现的方法。实现一个子类时，遇到抽象方法必须去做自己的实现。继承并实现接口时，要实现里面所有的方法，否则会报错。

:::

在 Flink API 调用过程中，绝大多数情况下都继承一个父类或接口。对于 Java 用户来说，如果继承一个接口，就要使用 `implements` 关键字；如果继承一个类，要使用 `extends` 关键字。对于 Scala 用户来说，绝大多数情况使用 `extends` 关键字就足够了。

## 重写与重载

### 重写

子类可以用自己的方式实现父类和接口的方法，比如前文提到的 move() 方法。子类的方法会覆盖父类中已有的方法，实际执行时，Java 会调用子类方法，而不是使用父类方法，这个过程被称为重写（Override）。在实现重写时，需要使用 `@Override` 注解（Annotation）。重写可以概括为，外壳不变，核心重写；或者说方法签名等都不能与父类有变化，只修改花括号内的逻辑。

虽然 Java 没有强制开发者使用这个注解，但是 `@Override` 会检查该方法是否正确重写了父类方法，如果发现其父类或接口中并没有该方法时，会报编译错误。像 IntelliJ IDEA 之类的集成开发环境也会有相应的提示，帮助我们检查方法是否正确重写。这里强烈建议开发者在继承并实现方法时养成使用 `@Override` 的习惯。

```java
public class ClassA implements Move {
    @Override
    public void move(){ 
        ...
    }
}
```

在 Scala 中，在方法前添加一个 `override` 关键字可以起到重写提示的作用。

```scala
class ClassA extends Move {
   override def move(): Unit = {
      ...
   }
}
```

### 重载

一个很容易和重写混淆的概念是重载（Overload）。重载是指，在一个类里有多个同名方法，这些方法名字相同、参数不同、返回类型不同。

```java
public class Overloading {

    // 无参数，返回值为 int 类型
    public int test(){
        System.out.println("test");
        return 1;
    }

    // 有一个参数
    public void test(int a){
        System.out.println("test " + a);
    }

    // 有两个参数和一个返回值
    public String test(int a, String s){
        System.out.println("test " + a  + " " + s);
        return a + " " + s;
    }
}
```

这段代码演示了名为 test() 的方法的多种不同的具体实现，每种实现在参数和返回值类型上都有区别。包括 Flink 在内，很多框架的源码和 API 应用了大量的重载，目的是给开发者提供多种不同的调用接口。

## 继承和多态小结

本节简单总结了 Java/Scala 的继承和多态基本原理和使用方法，包括数据建模、关键字的使用、方法的重写等。从 Flink 开发的角度来说，需要注意以下两点。

- 对于 Java 的一个子类，可以用 `extends` 关键字继承一个类，用 `implements` 关键字实现一个接口。如果需要覆盖父类的方法，则需要使用 `@Override` 注解。
- 对于 Scala 的一个子类，可以用 `extends` 关键字继承一个类或接口。如果需要覆盖父类的方法，则需要在方法前添加一个 `override` 关键字。
