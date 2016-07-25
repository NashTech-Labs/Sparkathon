name := "Sparkathon"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.0.0-preview",
  "org.apache.spark" %% "spark-sql" % "2.0.0-preview",
  "org.apache.spark" %% "spark-hive" % "2.0.0-preview",
  "org.apache.spark" %% "spark-streaming" % "2.0.0-preview",
  "org.apache.spark" %% "spark-mllib" % "2.0.0-preview",
  "org.scalatest" %% "scalatest" % "2.2.6")
    