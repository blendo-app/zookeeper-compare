package com.apirise.blendo.tools

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.collection.JavaConverters._

object ZookeeperCompare extends App {

  case class AppConfig(zk1: String = "", zk2: String = "", basePath: String = "", print: Boolean = false)

  def assertNodeEquality(cf1: CuratorFramework, cf2: CuratorFramework, path: String, print: Boolean): Boolean = {
    val exists1 = cf1.checkExists().forPath(path) != null
    val exists2 = cf2.checkExists().forPath(path) != null

    val assertion =
      if (exists1 && exists2) {
        val data1 = cf1.getData.forPath(path)
        val data2 = cf2.getData.forPath(path)
        if (data1 == null) data2 == null
        else new String(data1) == new String(data2)
      }
      else false

    if (print) println(s"${if (!assertion) "DIFF " else "EQ "}$path")
    assertion
  }

  def allPaths(cf: CuratorFramework, basePath: String): List[String] = {
    val children: List[String] = cf.getChildren.forPath(basePath).asScala.toList.map(pathFormat(basePath, _))
    children ::: children.flatMap(allPaths(cf, _))
  }

  def pathFormat(path: String, child: String): String = {
    if (path.endsWith("/")) path + child
    else path + "/" + child
  }

  def curatorClient(address: String): CuratorFramework =
    CuratorFrameworkFactory
      .builder()
      .connectString(address)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()

  val parser = new scopt.OptionParser[AppConfig]("zookeeper-compare") {

    head("zookeeper-compare: compare data equality of two zookeeper instances under a path")

    opt[String]("zk1").required().action(
      (zk1addr, conf) => conf.copy(zk1 = zk1addr)
    ).text("First zookeeper instance's address")

    opt[String]("zk2").required().action(
      (zk2addr, conf) => conf.copy(zk2 = zk2addr)
    ).text("Second zookeeper instance's address")

    opt[String]("path").withFallback(() => "/").action(
      (path, conf) => conf.copy(basePath = path)
    ).text("Path to compare equality under. Default: \"/\"")

    opt[Unit]('p', "print").action(
      (_, conf) => conf.copy(print = true)
    ).text("Enables printing of compared paths")
  }

  val appConfigOpt: Option[AppConfig] = parser.parse(args, AppConfig())

  if (appConfigOpt.isEmpty) System.exit(1)

  // This is our working configuration
  val appConfig = appConfigOpt.get

  val curatorFramework1 = curatorClient(appConfig.zk1)
  val curatorFramework2 = curatorClient(appConfig.zk2)

  curatorFramework1.start()
  curatorFramework2.start()


  val paths1 = allPaths(curatorFramework1, appConfig.basePath).sorted
  val paths2 = allPaths(curatorFramework2, appConfig.basePath).sorted


  val pathUnion = paths1.union(paths2).distinct

  val equals = {
    val pathEquality = pathUnion == paths1 && pathUnion == paths2

    if (pathEquality) {
      val nodeEqualities = pathUnion.map(assertNodeEquality(curatorFramework1, curatorFramework2, _, appConfig.print))
      !nodeEqualities.contains(false)
    }
    else false
  }

  if (equals) println("\n\nZookeeper instances are equal.")
  else {
    if (appConfig.print) {
      paths1.foreach { path =>
        if (!paths2.contains(path)) println("ZK1_ONLY " + path)
      }
      paths2.foreach { path =>
        if (!paths1.contains(path)) println("ZK2_ONLY " + path)
      }
    }

    println("\n\nZookeeper instances NOT equal.")
  }

  curatorFramework1.close()
  curatorFramework2.close()


}
