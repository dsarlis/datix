package gr.ntua.cslab.streamnet.bolts;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExclamationBolt.class.getName());
	OutputCollector _collector;
	
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topo, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
//    	LOG.info("Processing record: " + tuple.getStrin(0));
    	String out = tuple.getString(0) + "!";
    	_collector.emit(new Values(out));
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}

