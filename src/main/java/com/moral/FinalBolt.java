package com.moral;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Created by bin.shen on 30/10/2016.
 * 最终的结果打印bolt
 */
public class FinalBolt extends BaseRichBolt {

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
//        最终的结果打印bolt
        System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+"  final bolt ");
        Map<String,Integer> counts= (Map<String, Integer>) tuple.getValue(0);
        for(Map.Entry<String,Integer> kv:counts.entrySet()){
            System.out.println(kv.getKey()+"  "+kv.getValue());
        }
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}