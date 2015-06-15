package gr.ntua.cslab.streamnet.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SFlowBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	private String boltName;
	
	
	public SFlowBolt(String name) {
		this.boltName = name;
	}
	
	@Override
	public void execute(Tuple arg0) {
		
	}

	@Override
	public void prepare(Map conf, TopologyContext topo, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}
}
