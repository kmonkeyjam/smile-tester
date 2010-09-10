import sbt._
import com.twitter.sbt.StandardProject


class SmileLoadtestProject(info: ProjectInfo) extends StandardProject(info) {
  val smile = "net.lag" % "smile" % "0.8.13"
  val commons_pool = "org.apache.commons" % "commons-pool" % "1.3"
  val memcached = "com.meetup" % "memcached" % "1.0"
  val configgy = "net.lag" % "configgy" % "1.6.1"
  val ostrich = "com.twitter" % "ostrich" % "1.1.26"  //--auto--
  val util = "com.twitter" % "util" % "1.1-SNAPSHOT"
}
