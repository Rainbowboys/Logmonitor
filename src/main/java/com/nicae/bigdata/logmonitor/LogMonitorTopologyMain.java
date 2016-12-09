package com.nicae.bigdata.logmonitor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.nicae.bigdata.logmonitor.bolt.FilterBolt;
import com.nicae.bigdata.logmonitor.bolt.PrepareRecordBolt;
import com.nicae.bigdata.logmonitor.bolt.SaveMessageToMysql;
import com.nicae.bigdata.logmonitor.spout.RandomSpout;
import com.nicae.bigdata.logmonitor.spout.StringScheme;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

/**
 * 日志监控系统驱动类
 * 张城瑞
 */
public class LogMonitorTopologyMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //使用TopologyBuilder 创建Topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //发射随机数据
        topologyBuilder.setSpout("random-spout", new RandomSpout(new StringScheme()), 2);
        //topologyBuilder.setSpout("random-spout",new KafkaSpout(new SpoutConfig()),2);
        topologyBuilder.setBolt("filter-bolt", new FilterBolt(), 3).shuffleGrouping("random-spout");
        topologyBuilder.setBolt("prepareRecord-bolt", new PrepareRecordBolt(), 3).fieldsGrouping("filter-bolt", new Fields("appId"));
        topologyBuilder.setBolt("saveMessage-bolt", new SaveMessageToMysql(), 3).fieldsGrouping("prepareRecord-bolt", new Fields("record"));
        //设置topology的配置信息
        Config config = new Config();
        //设置worker 的数量

        //TOPOLOGY_DEBUG(setDebug),当他被设置true 时 ，storm会记录下组件发射的每一条信息
        //本地模式 这样做调试效果相当好 ，在集群运行时，这样做会降低集群的整体效率
        config.setDebug(true);


        //判断启动模式

        if (args != null && args.length > 0) {
            //定义你希望集群分配多少个工作进程给你来执行这个topology
            config.setNumWorkers(2);
            StormSubmitter.submitTopology("logMonitor", config, topologyBuilder.createTopology());
        } else {
            //启动本地模式
            //最大并行数目
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("logMonitor", config, topologyBuilder.createTopology());
        }
    }
}
