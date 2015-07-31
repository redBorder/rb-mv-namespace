package net.redborder.mvnamespace

object Utils {
  val filesURIPattern = "hdfs://hadoopnamenode.redborder.cluster:8020/rb/raw/data/%s/%s/hourly/*/*/*/*/*.gz"

  def getFilesURI(topic: String, namespace: String): String = {
    filesURIPattern.format(topic, namespace)
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
