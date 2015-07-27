package gr.ntua.cslab.streamnet;

import gr.ntua.cslab.streamnet.bolts.ExclamationBolt;
import gr.ntua.cslab.streamnet.bolts.SFlowBolt;
import gr.ntua.cslab.streamnet.bolts.SimpleBolt;
import gr.ntua.cslab.streamnet.shared.StreamNetStaticComponents;

import java.util.logging.Logger;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {
	
	private static final String TOPIC_NAME = "sflows";
	
	public static void main(String[] args) throws Exception {
		
        // create and start Storm Topology
        BrokerHosts brokerHosts = new ZkHosts("master:2181");
        
        StreamNetStaticComponents.TABLE_NAME = args[0];
        int splitSize = Integer.parseInt(args[6]);
        StreamNetStaticComponents.fullCached = Integer.parseInt(args[12]);
        int fullStore = Integer.parseInt(args[13]);
        String topology = args[14];
        
        TopologyBuilder builder = new TopologyBuilder();
        int spoutNo = Integer.parseInt(args[2]);
        int spoutPar = Integer.parseInt(args[3]);
        int boltNo = Integer.parseInt(args[4]);
        int boltPar = Integer.parseInt(args[5]);

        // initialize spouts
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
    	kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        for (int i = 1; i <= spoutNo; i ++) {
        	builder.setSpout("words", new KafkaSpout(kafkaConfig), spoutNo);
//        }
        
        if (topology.equals("kdtree")) {
        	BoltDeclarer bd = null;
//        	for (int i = 1; i <= boltNo; i ++) {
        		bd = builder.setBolt("worker", new SFlowBolt("worker", boltNo, splitSize, fullStore), boltNo);
//        		for (int j = 1; j <= spoutNo; j++) {
        		bd = bd.shuffleGrouping("words");
//        		}
//        		for (int k = 1; k <= boltNo; k++) {
        			bd = bd.directGrouping("worker");
//        		}
//        	}
        }
        else if (topology.equals("exclamation")) {
        	BoltDeclarer bd = null;
//        	for (int i = 1; i <= boltNo; i ++) {
        		bd = builder.setBolt("worker", new ExclamationBolt("worker", boltNo, splitSize), boltNo);
        		bd = bd.shuffleGrouping("words");
//        		for (int k = 1; k <= boltNo; k++) {
//        			if (i != k)
        		bd = bd.directGrouping("worker");
//        		}
//        	}
        }
        else {
        	BoltDeclarer bd = null;
        	for (int i = 1; i <= boltNo; i ++) {
        		bd = builder.setBolt("worker" + i, new SimpleBolt(), boltPar);
        		bd = bd.shuffleGrouping("words");
        	}
        }
        
        Config conf = new Config();
        conf.setDebug(false);
//        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, boltPar);

        if (args != null && args.length > 1) {
          // remote cluster
          conf.setNumWorkers(spoutNo * spoutPar + boltNo * boltPar);
          conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, Integer.parseInt(args[7]));
          conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Integer.parseInt(args[8]));
          conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Integer.parseInt(args[9]));
          conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, Integer.parseInt(args[10]));
          conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Integer.parseInt(args[11]));
          StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
          Logger.getLogger(Main.class.getName()).info("StreamNet is Started!");
        }
        else {

          // local mode
          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("test", conf, builder.createTopology());
          Logger.getLogger(Main.class.getName()).info("StreamNet is Started!");
          Utils.sleep(10000);
          cluster.killTopology("test");
          cluster.shutdown();
        }
    }
}
