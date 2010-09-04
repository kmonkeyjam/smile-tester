package com.twitter.smiletester

import net.lag.configgy.{ConfigMap, Configgy}
import net.lag.smile.{MemcacheClient, MemcacheServerTimeout}
import java.util.{Date, Timer, TimerTask}
import java.util.concurrent.CountDownLatch
import collection.jcl.ArrayList

class MemcacheTask(
    client: MemcacheClient[String],
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
          if (value == key) {
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

object Tester {
  def main(args: Array[String]) {
    Configgy.configure("config/test.conf")

    val config = Configgy.config.configMap("loadtest")
    val delay = config.getInt("delay_ms", 1)
    val numKeys = config.getInt("num_keys", 10)
    val numConns = config.getInt("num_conn", 10)
    val numRunsPerConn = config.getInt("num_runs_per_conn", 10)
    val numConnectionPools = config.getInt("num_pools", 1)
    val maxConnectionsToMemcache = config.getInt("max_conn", 900)

    
    val memcacheConfig = Configgy.config.configMap("memcache")
    memcacheConfig.setInt("num_connection_in_pool", Math.min(numConns / numConnectionPools, maxConnectionsToMemcache))

//    val numConnInPool = memcacheConfig.getInt("num_connection_in_pool", 1)
//    val client = new ClientPool(numConnInPool, memcacheConfig)

    val clientPool = new ArrayList[MemcacheClient[String]]

    val client = MemcacheClient.create(memcacheConfig)
    for (i <- 1 to numKeys) {
      val key = "TEST" + i
      client.set(key, key)
    }
    clientPool += client

    for (i <- 1 to numConnectionPools - 1) {
      clientPool += MemcacheClient.create(memcacheConfig)
    }

    val latch = new CountDownLatch(numConns)

    val tasks = new ArrayList[MemcacheTask]

    val start = System.currentTimeMillis + 3000
    val startTime = new Date(start)
    for (i <- 1 to numConns) {
      val timer = new Timer(true)
      val task = new MemcacheTask(clientPool(i % numConnectionPools), memcacheConfig, timer, numKeys, numRunsPerConn, latch)
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
    println("Delay: " + delay + "ms Num conn: " + numConns + " Num Pools: " + numConnectionPools, " Num memcache conn per client: " + memcacheConfig.getInt("num_connection_in_pool").get)
    println("Total successful: " + totalSuccess + " total unsuccessful: " + totalUnsuccess)
    println("Total elapsed time: " + (finish - start))
    if (totalSuccess > 0) println("Avg memcache read time for success: " + totalSuccessTime / totalSuccess)
    if (totalUnsuccess > 0) println("Avg memcache read time for failures: " + totalUnsuccessTime / totalUnsuccess)
    println(totalReq / elapsedTime  + " req/sec")
   }
}

