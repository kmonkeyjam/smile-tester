import sbt._
import com.twitter.sbt.StandardProject


class DBTesterProject(info: ProjectInfo) extends StandardProject(info) {
  val configgy = "net.lag" % "configgy" % "1.6.1"
  val ostrich = "com.twitter" % "ostrich" % "1.1.26"  //--auto--
  val passbird = "com.twitter" % "passbird" % "0.1-SNAPSHOT"  
}
