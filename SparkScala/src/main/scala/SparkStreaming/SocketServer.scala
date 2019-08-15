package SparkStreaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * 建立一个socket服务，间隔一定时间从文件中随机读取一行内容
  */
object SocketServer {
    private val rd = new java.util.Random()

    def rdInt(max: Int): Int ={
        rd.nextInt(max)
    }

    def main(args: Array[String]): Unit = {
        val fileName = args(0) // 读取的文件路径
        val port = args(1).toInt // socket端口号
        val interval = args(2).toLong // 读取文件内容的时间间隔：毫秒

        val reader = Source.fromFile(fileName)
        val lines = reader.getLines().toList
        reader.close()
        val length = lines.length
        val listener = new ServerSocket(port)
        while (true){ // 一直监听该socket端口
            val socket = listener.accept()
            new Thread(){
                override def run = {
                    val out = new PrintWriter(socket.getOutputStream, true)
                    while (true){
                        Thread.sleep(interval)
                        val content = lines(rdInt(length))
                        println(content)
                        out.write(content + "\n")
                        out.flush()
                    }
                    socket.close()
                }
            }.start()
        }
    }
}


//object SocketServer {
//    private val rd = new java.util.Random()
//
//    def rdInt(max: Int): Int ={
//        rd.nextInt(max)
//    }
//
//    def main(args: Array[String]): Unit = {
//        val port = 9999 // socket端口号
//        val interval = 1000 // 读取文件内容的时间间隔：毫秒
//
//        var num = 0
//        val listener = new ServerSocket(port)
//        while (true){ // 一直监听该socket端口
//            val socket = listener.accept()
//            new Thread(){
//                override def run = {
//                    val out = new PrintWriter(socket.getOutputStream, true)
//                    while (true){
//                        Thread.sleep(interval)
//                        println(num)
//                        out.write(num + "\n")
//                        out.flush()
//                        num += 1
//                    }
//                    socket.close()
//                }
//            }.start()
//        }
//    }
//}