package net.redborder.mvnamespace

object Utils {
  val filesURIPattern = "hdfs://hadoopnamenode.redborder.cluster:8020/rb/raw/data/%s/%s/hourly/*/*/*/*/*.gz"

  def getFilesURI(topic: String, namespace: String): String = {
    filesURIPattern.format(topic, namespace)
  }

  def getFilename(baseFilename: String, eventsCount: Int, config: Config): String = {
    var filename = baseFilename.replaceAll("/data/([a-zA-Z0-9_]+)/(\\d+)/hourly", "/data/$1/" + config.destination + "/hourly")
    filename = filename.replaceAll(".gz", "")
    val fileTokens = filename.split('.')
    fileTokens(5) = eventsCount.toString
    fileTokens(6) = "movedFrom"
    fileTokens(7) = config.source
    fileTokens.mkString(".")
  }

  // TODO: These fields are not checked and are unsafely casted! Refactor that!

  def getString(key: String, data: Map[String, String]) : String = {
    data.getOrElse(key, "N/A")
  }

  def getInt(key: String, data: Map[String, String]) : Int = {
    getString(key, data).toInt
  }

  def getLong(key: String, data: Map[String, String]) : Long = {
    getString(key, data).toLong
  }
}
