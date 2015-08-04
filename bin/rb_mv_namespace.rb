#!/usr/bin/ruby

require 'zk'
require "getopt/std"

opt = Getopt::Std.getopts("ht:n:d:w:f:s:")

SPARK_DIR = "/root/spark"
DUMBO_DIR = "/opt/rb/var/druid-dumbo"

def logit(text)
  printf("%s\n", text)
end

if opt["h"]
  logit "rb_mv_namespace.rb -t topic -n origin -d destination -w indexingWindow -f indexingOffset -s sensorList [-h]"
  logit "    -t topic           -> topic to move"
  logit "    -n origin          -> datasource origin"
  logit "    -d destination     -> datasource where the data will be copied and indexed"
  logit "    -w indexingWindow  -> hours that will be indexed"
  logit "    -f indexingOffset  -> offset from now used as window end"
  logit "    -s sensorList      -> comma-separated list of sensors that will be moved to the new datasource"
  logit "    -h                 -> print this help"
  logit ""
  logit "Example: rb_indexing_task.rb -t rb_flow -n 1527733215 -d 723412234 -w 720 -f 0 -s ASR,WLC"
  exit 0
end

topic = opt["t"]
origin = opt["n"]
destination = opt["d"]
window = opt["w"]
offset = opt["f"]
sensorList = opt["s"]

if (topic.nil? || origin.nil? || destination.nil? || window.nil? || offset.nil? || sensorList.nil?)
  logit "All the fields are required. Check your params and try again."
  exit 1
end

# Get overlords hostnames
hostname=`hostname`.strip
overlord=nil
zk_host="localhost:2181"
config=YAML.load_file('/opt/rb/etc/managers.yml')
if !config["zookeeper"].nil? or !config["zookeeper2"].nil?
  zk_host=((config["zookeeper"].nil? ? [] : config["zookeeper"].map{|x| "#{x}:2181"}) + (config["zookeeper2"].nil? ? [] : config["zookeeper2"].map{|x| "#{x}:2182"})).join(",")

  zk = ZK.new(zk_host)
  overlords = zk.children("/druid/discoveryPath/overlord").map{|k| k.to_s}.sort.uniq.shuffle
  zktdata,stat = zk.get("/druid/discoveryPath/overlord/#{overlords.first}")
  zktdata = YAML.load(zktdata)
  if zktdata["address"] and zktdata["port"]
    overlord="#{zktdata["address"]}:#{zktdata["port"]}".strip
  end
end

# Run the copy process and the reindexing process
system "#{SPARK_DIR}/bin/spark-submit --class net.redborder.mvnamespace.Application --master yarn-cluster /home/hadoop/rb-mv-namespace-1.0-SNAPSHOT-selfcontained.jar -t #{topic} -n #{origin} -d #{destination} -s #{sensorList}"
system "#{DUMBO_DIR}/bin/dumbo -d #{DUMBO_DIR}/conf/database.json -m verify -n #{hostname} -o #{overlord} -s #{DUMBO_DIR}/conf/sources.json -t #{topic} -a #{destination} -w #{window} -f #{offset} --zookeeper-path /discoveryPath"
