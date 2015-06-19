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

        builder.setSpout("words", new KafkaSpout(kafkaConfig), 1);
        
        builder.setBolt("worker1", new SFlowBolt("worker1"), 1).shuffleGrouping("words")
        	.directGrouping("worker1")
        	.directGrouping("worker2")
        	.directGrouping("worker3")
        	.directGrouping("worker4")
        	.directGrouping("worker5");
        
        builder.setBolt("worker2", new SFlowBolt("worker2"), 1).shuffleGrouping("words")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5");
        
        
        builder.setBolt("worker3", new SFlowBolt("worker3"), 1).shuffleGrouping("words")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5");
        
        builder.setBolt("worker4", new SFlowBolt("worker4"), 1).shuffleGrouping("words")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5");
        
        builder.setBolt("worker5", new SFlowBolt("worker5"), 1).shuffleGrouping("words")
    		.directGrouping("worker1")
    		.directGrouping("worker2")
    		.directGrouping("worker3")
    		.directGrouping("worker4")
    		.directGrouping("worker5");
//        builder.setBolt("exclaim", new ExclamationBolt(), 2).shuffleGrouping("words");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 1) {
          // remote cluster
          conf.setNumWorkers(Integer.parseInt(args[1]));

//          System.setProperty("storm.jar", "/opt/streamnet/StreamNet.jar");
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
