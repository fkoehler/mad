import sbt._
import sbt.Keys._

object MadBuild extends Build {

  lazy val mad = Project(
    id = "mad",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "mad",
      organization := "com.github.mad",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.1",
      libraryDependencies ++= Seq(
        "com.jsuereth" %% "scala-arm" % "1.3",
        "org.specs2" %% "specs2" % "1.14" % "test",
        "com.allanbank" % "mongodb-async-driver" % "1.2.0",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1"
      ),
      resolvers += "allanbank" at "http://www.allanbank.com/repo/",
      publishTo <<= version {
        (v: String) =>
          val base = "/Users/fab/dev/fkoehler-mvn-repo"
          if (v.trim.endsWith("SNAPSHOT"))
            Some(Resolver.file("file", new File(base + "/snapshots")))
          else
            Some(Resolver.file("file", new File(base + "/releases")))
      }
    )
  )

}
