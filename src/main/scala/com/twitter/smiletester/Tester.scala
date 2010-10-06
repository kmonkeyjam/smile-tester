package com.twitter.smiletester

import java.util.{Date, Timer, TimerTask}
import java.util.concurrent.CountDownLatch
import java.lang.Integer
import java.sql.ResultSet
import collection.jcl.ArrayList
import collection.mutable.ListBuffer
import net.lag.configgy.{RuntimeEnvironment, ConfigMap, Configgy}
import com.twitter.ostrich.{Service, ServiceTracker}
import net.lag.logging.Logger
import com.twitter.service.passbird.loadbalancer.QueryEvaluatorLoadBalancer

class LoadtestTask(
    queryEvaluator: QueryEvaluatorLoadBalancer,
    timer: Timer,
    numKeys: Int,
    numRuns: Int,
    latch: CountDownLatch) extends TimerTask {
  val random = new Random
  var totalSuccessfulRuns = 0
  var totalUnsuccessfulRuns = 0
  var totalElapsedSuccessfulRunTime = 0L
  var totalElapsedUnsuccessfulRunTime = 0L
  val query = "SELECT user_id, token, secret, client_application_id, invalidated_at, hashed_password FROM oauth_access_tokens LEFT JOIN users ON users.id = oauth_access_tokens.user_id WHERE token = ? AND (invalidated_at IS NULL OR invalidated_at > NOW())"

  override def run() {
    try {
      val token = "19-6OWobOFlEurdOiNBpxucg8M8sRVL01ilReGC2ZjE"
      val start = System.currentTimeMillis
      val key = ""
      val resp = Some("")
      val results = queryEvaluator.select(query, token)((row: ResultSet) => row.getString("secret"))
      val elapsed = System.currentTimeMillis - start
      totalSuccessfulRuns += 1
      totalElapsedSuccessfulRunTime += elapsed
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

    parseArgs(args.toList)

    val queryEvaluator = QueryEvaluatorLoadBalancer(
      config, Logger.get("QueryEvaluatorLoadBalancer"), Logger.get("QueryEvaluator"))

    val latch = new CountDownLatch(numConns)
    val tasks = new ArrayList[LoadtestTask]
    val start = System.currentTimeMillis + 3000
    val startTime = new Date(start)
    for (i <- 1 to numConns) {
      val timer = new Timer(true)
      val task = new LoadtestTask(queryEvaluator, timer, numKeys, numRunsPerConn, latch)
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
    println("Total successful: " + totalSuccess + " total unsuccessful: " + totalUnsuccess)
    println("Total elapsed time: " + (finish - start))
    if (totalSuccess > 0) println("Avg read time for success: " + totalSuccessTime / totalSuccess)
    if (totalUnsuccess > 0) println("Avg read time for failures: " + totalUnsuccessTime / totalUnsuccess)
    if (elapsedTime > 0) println(totalReq / elapsedTime  + " req/sec")

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
        parseArgs(xs)
      }
      case Nil => // skip
      case unknown :: _ => 
    }
  }
}