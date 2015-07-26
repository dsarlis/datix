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

public class ExclamationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(ExclamationBolt.class.getName());
	OutputCollector _collector;
	TopologyContext _topo;
	private int boltNo;
	private int randomNo;
	private String name;

	public ExclamationBolt(String name, int boltNo, int randomNo) {
		this.boltNo = boltNo;
		this.randomNo = randomNo;
		this.name = name;
	}
	
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topo, OutputCollector collector) {
      _collector = collector;
      _topo = topo;
    }

    @Override
    public void execute(Tuple tuple) {
    	if (!tuple.getString(0).contains("!")) {
    		Random ran = new Random();
    		int worker = -1;
    		for (int i = 0; i < randomNo; i++) {
    			worker = ran.nextInt(boltNo) + 1;
    		}
    		while (name.equals("worker" + worker))
    			worker = ran.nextInt(boltNo) + 1;
    		List<Integer> l = _topo.getComponentTasks("worker" + worker);
//    		LOG.info("List: " + l + " for worker: " + worker);
    		String out = tuple.getString(0) + "!";
    		LOG.info("Sending record: " + tuple.getString(0) + " to appropriate worker: worker" + worker);
    		_collector.emitDirect(l.get(0), new Values(out));
    		LOG.info("After emit");
    	}
    	else {
//    		LOG.info("Already processed record: " + tuple.getString(0));
    		Random ran = new Random();
    		int worker = -1;
    		for (int i = 0; i <randomNo; i++) {
    			worker = ran.nextInt(boltNo) + 1;
    		}
    	}
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}
