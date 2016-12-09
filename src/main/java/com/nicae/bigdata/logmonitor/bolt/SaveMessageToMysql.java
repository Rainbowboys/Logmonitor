package com.nicae.bigdata.logmonitor.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.nicae.bigdata.logmonitor.domain.Record;
import com.nicae.bigdata.logmonitor.utils.MonitorHandler;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by Rainbow on 2016/12/8.
 */
public class SaveMessageToMysql extends BaseBasicBolt{
    private static Logger logger = Logger.getLogger(SaveMessageToMysql.class);
    public void execute(Tuple input, BasicOutputCollector collector) {

        Record record = (Record) input.getValueByField("record");
        MonitorHandler.save(record);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
