name := "final_project_spark"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val doobieVersion = "0.7.0"

libraryDependencies := Seq(
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "commons-net" % "commons-net" % "3.6",
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "mysql" % "mysql-connector-java" % "5.1.47",
  "com.typesafe" % "config" % "1.3.4"
)