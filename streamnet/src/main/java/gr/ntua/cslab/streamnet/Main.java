package gr.ntua.cslab.streamnet;

import gr.ntua.cslab.streamnet.bolts.SFlowBolt;
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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {
	
	private static final String TOPIC_NAME = "sflows";
	
	public static void main(String[] args) throws Exception {
		
        // create and start Storm Topology
        BrokerHosts brokerHosts = new ZkHosts("master:2181");
        
        StreamNetStaticComponents.TABLE_NAME = args[0];
        StreamNetStaticComponents.splitSize = Integer.parseInt(args[3]);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        TopologyBuilder builder = new TopologyBuilder();
        int spoutPar = 1;
        int boltPar = 4;

        builder.setSpout("words1", new KafkaSpout(kafkaConfig), spoutPar);
        builder.setSpout("words2", new KafkaSpout(kafkaConfig), spoutPar);
        builder.setSpout("words3", new KafkaSpout(kafkaConfig), spoutPar);
        builder.setSpout("words4", new KafkaSpout(kafkaConfig), spoutPar);
        
        builder.setBolt("worker1", new SFlowBolt("worker1"), boltPar).shuffleGrouping("words1")
        	.shuffleGrouping("words2")
        	.shuffleGrouping("words3")
        	.shuffleGrouping("words4")
        	.directGrouping("worker1")
        	.directGrouping("worker2")
        	.directGrouping("worker3")
        	.directGrouping("worker4")
        	.directGrouping("worker5")
        	.directGrouping("worker6")
        	.directGrouping("worker7")
        	.directGrouping("worker8");
        
        builder.setBolt("worker2", new SFlowBolt("worker2"), boltPar).shuffleGrouping("words1")
        	.shuffleGrouping("words2")
        	.shuffleGrouping("words3")
        	.shuffleGrouping("words4")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5")
    		.directGrouping("worker6")
        	.directGrouping("worker7")
        	.directGrouping("worker8");
        
        
        builder.setBolt("worker3", new SFlowBolt("worker3"), boltPar).shuffleGrouping("words1")
        	.shuffleGrouping("words2")
        	.shuffleGrouping("words3")
        	.shuffleGrouping("words4")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5")
    		.directGrouping("worker6")
        	.directGrouping("worker7")
        	.directGrouping("worker8");
        
        builder.setBolt("worker4", new SFlowBolt("worker4"), boltPar).shuffleGrouping("words1")
        	.shuffleGrouping("words2")
        	.shuffleGrouping("words3")
        	.shuffleGrouping("words4")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5")
    		.directGrouping("worker6")
        	.directGrouping("worker7")
        	.directGrouping("worker8");
        
        builder.setBolt("worker5", new SFlowBolt("worker5"), boltPar).shuffleGrouping("words1")
        	.shuffleGrouping("words2")
        	.shuffleGrouping("words3")
        	.shuffleGrouping("words4")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5")
    		.directGrouping("worker6")
        	.directGrouping("worker7")
        	.directGrouping("worker8");
//        builder.setBolt("exclaim", new ExclamationBolt(), 2).shuffleGrouping("words");

        builder.setBolt("worker6", new SFlowBolt("worker6"), boltPar).shuffleGrouping("words1")
    	.shuffleGrouping("words2")
    	.shuffleGrouping("words3")
    	.shuffleGrouping("words4")
		.directGrouping("worker1")
		.directGrouping("worker2")
		.directGrouping("worker3")
		.directGrouping("worker4")
		.directGrouping("worker5")
		.directGrouping("worker6")
    	.directGrouping("worker7")
    	.directGrouping("worker8");
        
        builder.setBolt("worker7", new SFlowBolt("worker7"), boltPar).shuffleGrouping("words1")
    	.shuffleGrouping("words2")
    	.shuffleGrouping("words3")
    	.shuffleGrouping("words4")
		.directGrouping("worker1")
		.directGrouping("worker2")
		.directGrouping("worker3")
		.directGrouping("worker4")
		.directGrouping("worker5")
		.directGrouping("worker6")
    	.directGrouping("worker7")
    	.directGrouping("worker8");
        
        builder.setBolt("worker8", new SFlowBolt("worker8"), boltPar).shuffleGrouping("words1")
    	.shuffleGrouping("words2")
    	.shuffleGrouping("words3")
    	.shuffleGrouping("words4")
		.directGrouping("worker1")
		.directGrouping("worker2")
		.directGrouping("worker3")
		.directGrouping("worker4")
		.directGrouping("worker5")
		.directGrouping("worker6")
    	.directGrouping("worker7")
    	.directGrouping("worker8");
        
        Config conf = new Config();
        conf.setDebug(true);
//        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);

        if (args != null && args.length > 1) {
          // remote cluster
          conf.setNumWorkers(Integer.parseInt(args[1]));
          StormSubmitter.submitTopologyWithProgressBar(args[2], conf, builder.createTopology());
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
