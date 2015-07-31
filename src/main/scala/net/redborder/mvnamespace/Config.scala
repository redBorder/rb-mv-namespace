package net.redborder.mvnamespace

case class Config(source: String = "not_deployment_uuid",
                  destination: String = "",
                  topic: String = "rb_flow_post",
                  sensors: Seq[String] = Seq())