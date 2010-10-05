package com.twitter.smiletester

import java.util.{Date, Timer, TimerTask}
import java.util.concurrent.CountDownLatch
import java.lang.Integer
import collection.jcl.ArrayList
import collection.mutable.ListBuffer
import net.lag.smile._
import com.meetup.memcached.{SockIOPool, MemcachedClient}
import net.lag.configgy.{RuntimeEnvironment, ConfigMap, Configgy}
import com.twitter.ostrich.{Service, ServiceTracker}

class MemcacheTask(
    client: MemcachePool,
    memcacheConfig: ConfigMap,
    timer: Timer,
    numKeys: Int,
    numRuns: Int,
    latch: CountDownLatch) extends TimerTask {
  val random = new Random
  var totalSuccessfulRuns = 0
  var totalUnsuccessfulRuns = 0
  var totalElapsedSuccessfulRunTime = 0L
  var totalElapsedUnsuccessfulRunTime = 0L

  override def run() {
    try {
      val key = "TEST" + (random.nextInt(numKeys) + 1)
      val start = System.currentTimeMillis
      val resp = client.get(key)
      val elapsed = System.currentTimeMillis - start
      resp match {
        case Some(value) => {
          if (new String(value) == key) {
            // println(elapsed)
            totalSuccessfulRuns += 1
            totalElapsedSuccessfulRunTime += elapsed
          } else {
            totalUnsuccessfulRuns += 1
            totalElapsedUnsuccessfulRunTime += elapsed
          }
        }
        case None => totalUnsuccessfulRuns += 1
      }
    } catch {
      case e => totalUnsuccessfulRuns += 1
    } finally {
      if (totalSuccessfulRuns + totalUnsuccessfulRuns >= numRuns) {
        // println("Success: " + totalSuccessfulRuns + " Unsuccessful: " + totalUnsuccessfulRuns)
        // println("Success: " + totalElapsedSuccessfulRunTime + " Unsuccessful: " + totalElapsedUnsuccessfulRunTime)
        timer.cancel
        latch.countDown
      }
    }
  }
}

object Tester extends Service {
  val memcacheHosts = new ListBuffer[String]

  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.configFilename = "config/test.conf"
    runtime.load(args)

    Configgy.configure("config/test.conf")

    val config = Configgy.config.configMap("loadtest")
    val delay = config.getInt("delay_ms", 1)
    val numKeys = config.getInt("num_keys", 10)
    val numConns = config.getInt("num_conn", 10)
    val numRunsPerConn = config.getInt("num_runs_per_conn", 10)
    val numConnectionPools = config.getInt("num_pools", 1)
    val maxConnectionsToMemcache = config.getInt("max_conn", 900)
    val numConnInPool = config.getInt("num_smile_pools", 350)

    parseArgs(args.toList)
    
    val memcacheConfig = Configgy.config.configMap("memcache")
    memcacheConfig.setInt("num_connection_in_pool", Math.min(numConns / numConnectionPools, maxConnectionsToMemcache))

    if (!memcacheHosts.isEmpty) {
      memcacheConfig.setList("servers", memcacheHosts.toList)
    }


    MemcachePool(memcacheConfig)

    val client = new MemcachePool(numConnInPool, memcacheConfig)

    for (i <- 1 to numKeys) {
      val key = "TEST" + i
      client.set(key, key.getBytes, 0, 7200)
    }

    val latch = new CountDownLatch(numConns)

    val tasks = new ArrayList[MemcacheTask]

    val start = System.currentTimeMillis + 3000
    val startTime = new Date(start)
    for (i <- 1 to numConns) {
      val timer = new Timer(true)
      val task = new MemcacheTask(client, memcacheConfig, timer, numKeys, numRunsPerConn, latch)
      timer.scheduleAtFixedRate(task, startTime, delay)
      tasks += task
    }

    latch.await()
    val finish = System.currentTimeMillis

    var totalSuccess = 0
    var totalUnsuccess = 0
    var totalSuccessTime = 0L
    var totalUnsuccessTime = 0L

    tasks foreach { a =>
      totalSuccess += a.totalSuccessfulRuns
      totalUnsuccess += a.totalUnsuccessfulRuns
      totalSuccessTime += a.totalElapsedSuccessfulRunTime
      totalUnsuccessTime += a.totalElapsedUnsuccessfulRunTime
    }

    val totalReq = (totalSuccess + totalUnsuccess).toLong
    val elapsedTime = (finish - start) / 1000
    println(config)
    println(memcacheConfig)
    println("Total successful: " + totalSuccess + " total unsuccessful: " + totalUnsuccess)
    println("Total elapsed time: " + (finish - start))
    if (totalSuccess > 0) println("Avg memcache read time for success: " + totalSuccessTime / totalSuccess)
    if (totalUnsuccess > 0) println("Avg memcache read time for failures: " + totalUnsuccessTime / totalUnsuccess)
    println(totalReq / elapsedTime  + " req/sec")

    ServiceTracker.register(this)
    ServiceTracker.startAdmin(Configgy.config, runtime)    
  }

  def quiesce() {
    // what to do here? stop jetty?
  }

  def shutdown() {
    // what to do here? stop jetty?
  }
  
  def parseArgs(args: List[String]): Unit = {
    args match {
      case "-m" :: host :: xs => {
        memcacheHosts += host
        parseArgs(xs)
      }
      case Nil => // skip
      case unknown :: _ => 
    }
  }
}