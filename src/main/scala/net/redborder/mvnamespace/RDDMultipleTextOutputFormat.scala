package net.redborder.mvnamespace

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val keyStr = key.asInstanceOf[String]
    val data = value.asInstanceOf[String]
    // val namespaceDst = data("namespace_uuid")
    // keyStr.replaceAll("/data/([a-zA-Z0-9_]+)/(\\d+)/hourly", "/data/$1/" + namespaceDst)
    val newFilename = keyStr.replace("/rb/raw/data", "/rb/raw/moved")
    newFilename + "/" + name
  }
}