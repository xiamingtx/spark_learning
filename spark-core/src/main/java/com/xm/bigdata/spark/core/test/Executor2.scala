package com.xm.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Executor2 {
  def main(args: Array[String]): Unit = {

    // 1. 启动服务器 接收数据
    val server = new ServerSocket(8888)

    println("服务器启动 等待接收数据")

    // 等待客户端的连接
    val client:Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn: ObjectInputStream = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val res: List[Int] = task.compute()

    println("计算结点[8888]计算的结果为:  " + res)
    objIn.close()
    client.close()
    server.close()
  }
}
