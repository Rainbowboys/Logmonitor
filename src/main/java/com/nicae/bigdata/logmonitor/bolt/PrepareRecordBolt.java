package com.nicae.bigdata.logmonitor.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.nicae.bigdata.logmonitor.domain.Message;
import com.nicae.bigdata.logmonitor.domain.Record;
import com.nicae.bigdata.logmonitor.utils.MonitorHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 *
 */
public class PrepareRecordBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(PrepareRecordBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        Message message = (Message) input.getValueByField("message");
        String appid = (String) input.getValueByField("appId");
        if (MonitorHandler.trigger(message)) {
            MonitorHandler.notifly(appid, message);
            //将触发规则的信息进行通知
            Record record = new Record();
            try {
                BeanUtils.copyProperties(record, message);
                collector.emit(new Values(record));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }
}
