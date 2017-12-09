
lazy val root = (project in file(".")).
  settings(
    name := "spark-test",
    organization := "com.semrush",
    version := "0.0.1",

    scalaVersion := "2.11.7",
    scalacOptions += "-target:jvm-1.8",
    javaOptions += "-Xmxx4048m",
    ivyScala := ivyScala.value map {
      _.copy(overrideScalaVersion = true)
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided",
      "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided",
      "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
      "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided"
    ),
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
  )
