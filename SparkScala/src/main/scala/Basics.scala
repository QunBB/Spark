val str = "a" // val声明不可变变量（指向指针不可变），不可重新赋值，必须初始化 
var str : String = "a" // var声明可变变量，指定String类型 
val str : java.lang.String = "a"

// Range
1 to 5 // (1,2,3,4,5)
1 until 5 // (1,2,3,4)
0.5f to 5.9f by 0.8f

// if、while与java用法相同
for (i <- 1 to 5 if i%2==0) println(i)
for (i <- 1 to 5; j <- 1 to 5) println(i*j)
// 将for循环的值保存到变量中
val res = for (i <- 1 to 5 if i%2==0) yield {println(i); i}

// 读取控制台输入
import io.StdIn._

var v = readint()
var s = readLine("请输入：") // 参数为提示语

printf("output: %s, %d", "abc", 1)

// 写入文件
import java.io.PrintWriter

val out = new PrintWriter("output.txt")
for (i <- 1 to 5) out.println(i)
out.close()

// 读取文件
import scala.io.Source

val in = Source.fromFile("output.txt")
val lines = in.getLines
for (line <- lines) println(line)

// 异常捕获，与python相同，所有异常都不受检查
import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

try {
    val file = new FileReader("input.txt")
} catch {
    case e: FileNotFoundException =>
        println(e)
    case e: IOException =>
        println(e)
} finally {
    file.close()
}

// 容器
import scala.collection.mutable // 存放可变容器
import scala.collection.immutable //存放不可变容器

// List不可变
var strList = List("a", "b", "c")
strList(0)
strList.head // list的头部
strList.tail // list除头部以外的所有元素
val list = "d"::strList // 将"d"与list拼接

// set可变，也可以是不可变。默认是不可变的
var set = Set("a", "b")
set += "c" // 此时的操作是将set的指针重新指向set+"c"的地址

//Map可变+不可变
import scala.collection.mutable.Map

val map = Map("a" -> "1", "b" -> "2")
map("a") // key不存在时会报错
map.get("a") // key不存在时返回None
val v = if (map.contains("a")) map("a") else 0
map("c") = "3"
map += ("d" -> "4", "e" -> "5")
for ((k, v) <- map) println(k, v)
for (k <- map.keys) println(k) // 这里keys是方法，没有参数可以不写括号

// 迭代器
val iter = Iterator("a", "b", "c") // 新建迭代器
while (iter.hasNext) {
    println(iter.next())
}
for (e <- iter) {
    println(e)
}
// 迭代器访问集合
val list = List(1, 2, 3, 4, 5)
val g = list grouped 3 // 等同g.grouped(3)
g.next() // [1, 2, 3]
g.next() // [4, 5]
val s = list sliding 3
s.next() // [1, 2, 3]
s.next() // [2, 3, 4]

// 数组，可变，有索引
val arr = new Array[Int](3)
arr(0) = 1
val arr = Array(1, 2, 3)
val matrix = Array.ofDim[Int](3,4)
matrix(0)(1)
// 长度不变数组
val arr = ArrayBuffer(10, 20)
arr += 30
arr.insert(50, 60)
arr -= 60
var temp = arr.remove(2) // 删除索引为2的元素

// 元组
val tuple = (1, 2, "3")
tuple._1 // 访问元组的第一个元素

// 集合操作
// Map, flatMap, filter, reduce, fold
val list = List(1, 2, 3)
list.foreach(each => println(each))
list.map(x => x + 1)
list.flatMap(x => List(x, x*2)) // 对每个元素执行传入的函数并返回一个集合，然后将所有集合flat，[1, 2, 2, 4, 3, 6]
val map = Map("a" -> 1, "b" -> 2)
map.filter(kv => kv._2 == 2)
list.reduceLeft(_ + _) // 从左到右reduce
list.fold(10)(_ * _) // 10*1*2*3

// 类
class test(val name: String) { // 主构造器
    // 此时的name会自动编译为test类的属性
}

class Counter {
    private var privateV = 0
    private var v = 0

    // 构造器
    def this(value: Int) { // 第一个辅助构造器
        this() // 必须调用主构造器
        this.v = value
    }
    def this(v1: Int, v2: Int) { // 第二个辅助构造器
        this(v1) // 必须调用主构造器或第一个辅助构造器
        this.privateV = v2
    }

    def increment(): Unit = {v += 1} // 无返回值的方法
    def increment(): Unit = v += 1 // 作用同上
    def increment(): {v += 1} // 同上
    def current(): Int = {v} // 返回v
    def add(setp : Int) : Unit = {v += step}
    // get和set方法，调用时与python一致
    def v = privateV
    def v_ = (newV: Int) {privateV = newV}
}
// 新建对象
val myCounter = new Counter // 无参时可以省略
myCounter.increment
// 无法编译，可执行: scala xx.scala
// :load xxx.scala

object RunCounter {
    def main(args: Array[String]) {
        val myCounter = new Counter
        myCounter.increment
    }
}
// 需要编译: scalac xxx.scala
// 执行：scala -classpath . RunCounter

// 单例对象，类似java的工具类，都是静态方法
object Person {
    private var id = 0
    def add() = {
        id += 1
        id
    }
}
println(Person.add())

// 伴生对象
class Person { // 伴生类
    private val id = Person.addId() // 新建伴生对象时调用的方法
private var name = ""
    def this(name: String) {
        this()
        this.name = name
    }
    def info() {println(name, id)}
}
object Person { // 伴生对象，名称必须一致，且在同个文件
    private var ID = 0
    private def addId() = {
        ID += 1
        ID
    }
    def main(args: Array[String]) {
        val p1 = new Person("a")
        val p2 = new Person("b")
        p1.info() // "a", 1
        p2.info() // "b", 2，因为是静态方法
    }
}

// apply和update方法
// 用括号传递给变量（对象）一个或多个参数时，会转化成对apply方法的调用
// 对带括号并传递参数的对象进行赋值时，会转化为对update方法的调用

class MyObject {
    def apply(name: String) {
        println("调用apply方法: ", name)
    }
}
val myObject = new MyObject()
println(myObject("test")) // 此时会调用对象的apply方法
// 单例对象的apply方法
object MyObject {
    def apply(name: String) {
        println("调用apply方法")
        name
    }
}
val myObject = MyObject("test") // 此时会调用apply方法，打印"调用apply方法"，并返回"test"
println(myObject) // 打印"test"

val arr = Array[String](3)
arr(0) = "a" // 此时等同于arr.update(0, "a")，调用update方法

// 抽象类
abstract class Person {
    val name : String // 无初始值为抽象属性，必须声明类型
    def info() // 抽象方法
    def test() {println("test")} // 非抽象方法
}
// 继承抽象类
class Man extends Person {
    override name = "hhb"
    def info() {println(name)}
    override test() {println("Man_test")}
}
// 特质，类似java的接口，不同的是可以有非抽象方法
trait Car {
    var id
    def getId(): Int
}
// 继承多特质：extends .. with .. with ..

// 匹配模式
val num = 1
val str = num match {
    case 1 => "a"
    case 2 => "b"
    case _ => "c" // 前面都未匹配时
}
val str = num match {
    case 1 => "a"
    case 2 => "b"
    case i: Int => i // 类型匹配(i就是num)
    case _ if (e%2==0): e+2 // 逻辑匹配 
    case unexpected => unexpected + "|c" // 前面都未匹配时：4|c
}
// case类
case class Car(brand: String, price: Int)
val c1 = new Car("a", 10)
val c2 = new Car("b", 20)
for (c <- List(c1, c2)) {
    car match{
        case Car("a", 10) => println("a", 10)
        case Car(brand, price) => println(brand, price) // 其他case类
    }
}

// Option[T]，实际也是容器，要么只有一个元素，包装在Some中返回，要么就不存在元素，返回None
val map = Map("a" -> 1)
val v = map.get("a") // Option[Int] = Some(1)
val v = map.get("b") // Option[Int] = None
v.getOrElse("No Way") // 不为None时，返回v，为None时返回传入参数

// 函数
val add: Int => Int = {(value) => value + 1} // // lambda函数
val add = (value: Int) => value + 1

// 占位符
val list = List(-1, -2, 1, 2)
list.filter(x => x > 0)
list.filter(_ > 0) // 作用同上
val add = (_: Int) + (_: Int)