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

  override def run() {
    try {
      val key = "TEST" + (random.nextInt(numKeys) + 1)
      val resp = client.get(key)
      resp match {
        case Some(value) => {
          if (value == key) {
            totalSuccessfulRuns += 1
          } else {
            totalUnsuccessfulRuns += 1
          }
        }
        case None => totalUnsuccessfulRuns += 1
      }
    } catch {
      case e => totalUnsuccessfulRuns += 1
    } finally {
      if (totalSuccessfulRuns + totalUnsuccessfulRuns >= numRuns) {
        println("Success: " + totalSuccessfulRuns + " Unsuccessful: " + totalUnsuccessfulRuns)
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
    val rate = config.getInt("rate", 1)
    val numKeys = config.getInt("num_keys", 10)
    val numConns = config.getInt("num_conn", 10)
    val numRunsPerConn = config.getInt("num_seconds", 1) * rate

    val memcacheConfig = Configgy.config.configMap("memcache")

    val client = MemcacheClient.create(memcacheConfig)
    for (i <- 1 to numKeys) {
      val key = "TEST" + i
      client.set(key, key)
    }

    val latch = new CountDownLatch(numConns)

    val tasks = new ArrayList[MemcacheTask]

    val start = System.currentTimeMillis + 3000
    val startTime = new Date(start)
    for (i <- 1 to numConns) {
      val timer = new Timer(true)
      val task = new MemcacheTask(client, memcacheConfig, timer, numKeys, numRunsPerConn, latch)
      timer.scheduleAtFixedRate(task, startTime, 1000 / rate)
      tasks += task
    }

    latch.await()
    val finish = System.currentTimeMillis

    var totalSuccess = 0
    var totalUnsuccess = 0
    tasks foreach { a =>
      totalSuccess += a.totalSuccessfulRuns
      totalUnsuccess += a.totalUnsuccessfulRuns
    }

    val totalReq = (totalSuccess + totalUnsuccess).toLong
    val elapsedTime = (finish - start) / 1000
    println("Total successful: " + totalSuccess + " total unsuccessful: " + totalUnsuccess)
    println("Total elapsed time: " + (finish - start))
    println(totalReq / elapsedTime  + " req/sec")

    client.shutdown
  }
}

