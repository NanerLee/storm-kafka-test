package me.nanerlee.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class AddNumBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Integer num;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        num = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getStringByField("key");
//        String msg = tuple.getStringByField("message") + "-" + num.toString();
        String msg = tuple.getStringByField("value") + "-" + num.toString();
        collector.emit(tuple, new Values(key, msg));
        collector.ack(tuple);
        ++num;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }
}