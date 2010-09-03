import sbt._
import com.twitter.sbt.StandardProject


class SmileLoadtestProject(info: ProjectInfo) extends StandardProject(info) {
  val smile = "net.lag" % "smile" % "0.8.13"
}
