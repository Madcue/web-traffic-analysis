package com.twq.spark.web.external

import com.github.rholder.retry.{RetryerBuilder, StopStrategies, WaitStrategies}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

import java.util.concurrent.{Callable, TimeUnit}

object HbaseConnectionFactory {
  private val logger = LoggerFactory.getLogger(HbaseConnectionFactory.getClass)
  private lazy val zks = Option(System.getProperty("web.etl.hbase.zk.quorums")) //zookeeper 通过spark submit命令提供连接参数:--conf spark.executor.extraJavaOptions="-Dweb.省略" or spark.driver.extraJavaOptions
    //sys The package object scala.sys contains methods for reading and altering core aspects of the virtual machine as well as the world outside of it.
    .getOrElse(sys.error("must set parameter web.etl.hbase.zk.quorums"))
  private val zkPort = System.getProperty("web.etl.hbase.zk.port", "2181")
  val hbaseTableNamespace = System.getProperty("web.etl.hbase.namespace", "default")
  /** HBaseConfiguration.create() --->
    * public static Configuration addHbaseResources(Configuration conf) {
    * conf.addResource("hbase-default.xml");
    * conf.addResource("hbase-site.xml");
    * checkDefaultsVersion(conf);
    * HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
    * return conf;
    * }
    *
    * public static Configuration create() {
    * Configuration conf = new Configuration();
    * conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    * return addHbaseResources(conf);
    * }
    */
  private lazy val hbaseConfig = { //设置hbase Configuration,供Hbase静态工厂方法createConnection(hbaseConfig)使用
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf
  }
  private val lock = new Object()



  private var connection = createHbaseConnectionWithRetry

  def getHbaseConn = {
    if (isNormalHbaseConn(connection)) {//连接不为空 不关闭 不中止(死机)
      connection
    } else {
      lock.synchronized {//保证线程安全 并尝试获取或重连HBase连接,不能使用造成死锁的锁
        /**
          * synchronized:jvm层面 获取锁的线程执行完同步代码，释放锁 线程执行发生异常，jvm会让线程释放锁 有锁线程阻塞想获取需等待 无法判断锁状态 少量同步
          * Lock:         类层面 finally代码块中必须释放锁 ,可以不用一直等待尝试其他方式获取锁,大量同步
          */
        if (isNormalHbaseConn(connection)) {
          connection
        } else {
          val newConn = createHbaseConnectionWithRetry
          connection = newConn
          newConn
        }
      }
    }
  }

  private def isNormalHbaseConn(conn: Connection): Boolean = {
    conn != null && !conn.isClosed && !conn.isAborted
  }


  private def createHbaseConnectionWithRetry = {
    //Java Callable接口
    //返回结果并可能抛出异常的任务。实现者定义了一个不带参数的方法，称为call
    val callable = new Callable[Connection]() {
      //def call() Throws:
      //Exception – if unable to compute a result
      override def call(): Connection = {
        logger.info("creating hbase connection for etl")
        ConnectionFactory.createConnection(hbaseConfig)
      }
    }
    //import com.github.rholder.retry.RetryerBuilder
    val retryer = RetryerBuilder.newBuilder[Connection]()
      .retryIfException()//异常重试
      ////等待策略,该策略在第一次尝试失败后休眠的时间呈指数级增长，在每次尝试失败后休眠的时间呈指数级增长，直到maximumTime。重试之间的等待时间可以由乘数控制。
      .withWaitStrategy(WaitStrategies.exponentialWait(1000, 5, TimeUnit.MINUTES))
      // stopAfterAttempt 返回一个停止策略，在N次失败的尝试后停止。
      .withStopStrategy(StopStrategies.stopAfterAttempt(Integer.getInteger("wd.etl.hbase.connection.retryTimes", 4)))
      .build()
    /**
      * 执行给定的可调用对象。如果拒绝断言接受该尝试，
      * 则使用停止策略来决定是否必须进行新的尝试。
      * 然后使用等待策略来决定需要多长时间休眠，
      * 然后再进行一次尝试
      */
    retryer.call(callable)
  }

  //注册关机钩子,响应两种事件
  // 1.正常程序退出(最后一个非守护线程退出或System.exit),在JVM结束任务时关闭JVM进程,
  // 当用户中断(例如输入^C)或系统范围的事件(例如用户注销或系统关闭)时，虚拟机将被终止
  //获取runtime object对象Runtime.getRuntime,调用注册钩子实例方法
  Runtime.getRuntime.addShutdownHook(new Thread(){//关机钩子是一个线程
    override def run(): Unit = {
      if (isNormalHbaseConn(connection)) {
        logger.info("closing hbase connection for etl")
        connection.close()
      }

    }
  })



}
