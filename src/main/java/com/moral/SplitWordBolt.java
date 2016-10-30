package com.moral;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bin.shen on 30/10/2016.
 * 简单的按照空格进行切分后，发射到下一阶段bolt
 */
public class SplitWordBolt extends BaseRichBolt {

    Map<String,Integer> counts=new HashMap<>();

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence=tuple.getString(0);
//        System.out.println("线程"+Thread.currentThread().getName());
//        简单的按照空格进行切分后，发射到下一阶段bolt
        for(String word:sentence.split(" ") ){
            outputCollector.emit(new Values(word));//发送split
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明输出的filed
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}