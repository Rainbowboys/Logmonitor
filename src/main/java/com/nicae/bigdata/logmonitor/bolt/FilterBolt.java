package com.nicae.bigdata.logmonitor.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.nicae.bigdata.logmonitor.domain.Message;
import com.nicae.bigdata.logmonitor.utils.MonitorHandler;
import org.apache.log4j.Logger;

import java.util.Map;


/**
 * 过滤消息
 */
//BaseRichBolt 需要手动调ack方法，BaseBasicBolt由storm框架自动调ack方法
public class FilterBolt extends BaseBasicBolt {


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        // 获取KafkaSpout发送出来的数据
        String line = input.getString(0);
        //处理日志信息
        Message message = MonitorHandler.parser(line);
        //消息不为空是发射出去
        if (message == null) {
            return;
        } else {
            collector.emit(new Values(message.getAppId(), message));
        }
        //定时更新规则信息


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("appId", "message"));
    }
}
