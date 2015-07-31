package net.redborder.mvnamespace

import net.redborder.mvnamespace.Utils._
import net.redborder.mvnamespace.Dimension._
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.JsonMethods._
import scopt.OptionParser

object Application {
  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("mvnamespace") {
      head("mvnamespace", "1.0")
      opt[String]('n', "source") action { (x, c) => c.copy(source = x) }
      opt[String]('d', "destination") required() action { (x, c) => c.copy(destination = x) }
      opt[String]('t', "topic") action { (x, c) => c.copy(topic = x) }
      opt[Seq[String]]('s', "sensor") required() valueName "<sensor1>,<sensor2>..." action {
        (x, c) => c.copy(sensors = x) } text "sensors to move"
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case None =>
    }
  }

  def run(config: Config): Unit = {
    implicit lazy val formats = DefaultFormats

    val conf = new SparkConf().setAppName("mvnamespace").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)

    sc.wholeTextFiles(getFilesURI(config.topic, config.source))
      .map(data => {
        val parsed = parse(data._2).extract[Map[String, String]]
        (data._1, parsed)
      })
      .filter(x => {
        val sensorName = getString(SENSOR_NAME, x._2)
        config.sensors.contains(sensorName)
      })
      .map(x => {
        val resultMap = x._2 + ("namespace_uuid" -> config.destination)
        val json = write(resultMap)
        (x._1, json)
      })
      .saveAsHadoopFile("/rb/raw/" + sc.applicationId, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
  }
}

