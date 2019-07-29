package semantix.test.commons.handlers.spark

import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Spark {

  val session: SparkSession = this.setSession()

  def setSession(): SparkSession = {
    val conf: SparkConf = new SparkConf()

    SparkSession
      .builder()
      .config(conf)
      .appName("Teste Spark")
      .getOrCreate()
  }

  def getSession: SparkSession = {
    this.session
  }

  def getContext: SparkContext = {
    this.getSession.sparkContext
  }

  def getContextConf: SparkConf = {
    this.getContext.getConf
  }

  def setLogLevel(level: Level): Unit = {
    this.getContext.setLogLevel(level.toString)
  }

  def stop(): Unit = {

    this.getContext.stop()
    this.getSession.stop()
  }
}