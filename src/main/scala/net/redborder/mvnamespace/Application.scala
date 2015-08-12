package net.redborder.mvnamespace

import net.redborder.mvnamespace.Utils._
import net.redborder.mvnamespace.Dimension._
import org.apache.hadoop.io.compress.GzipCodec
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
      opt[Seq[String]]('s', "sensor_uuid") required() valueName "<sensorUUID_1>,<sensorUUID_2>..." action {
        (x, c) => c.copy(sensors = x) } text "sensors_uuid to move"
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
      .flatMap(data => {
        val maps = data._2.split("\n")
        maps.map(x => (data._1, x, maps.size))
      })
      .map(data => {
        val parsed = parse(data._2).extract[Map[String, String]]
        (data._1, data._3, parsed)
      })
      .filter(x => {
        val sensorUUID = getString(SENSOR_UUID, x._3)
        config.sensors.contains(sensorUUID)
      })
      .map(x => {
        val resultMap = x._3 + ("namespace_uuid" -> config.destination)
        val json = write(resultMap)
        val key = getFilename(x._1, x._2, config)
        (key, json)
      })
      .saveAsHadoopFile("/rb/raw/moved/" + sc.applicationId, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
  }
}

