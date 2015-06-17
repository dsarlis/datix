package gr.ntua.cslab.streamnet;

import gr.ntua.cslab.streamnet.bolts.ExclamationBolt;
import gr.ntua.cslab.streamnet.bolts.SFlowBolt;

import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.bridge.SLF4JBridgeHandler;

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
	
	private static void configureLogger() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        InputStream logPropertiesStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
        PropertyConfigurator.configure(logPropertiesStream);
        Logger.getLogger(Main.class.getName()).info("Logger configured");
    } 
	
	public static void main(String[] args) throws Exception {
        configureLogger();

        // create and start Storm Topology
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("words", new KafkaSpout(kafkaConfig), 3);
//        builder.setBolt("worker1", new SFlowBolt("worker1", "sflows_with_tree_partitioned"), 3).shuffleGrouping("word");
//        builder.setBolt("worker1", new SFlowBolt("worker2", ""), 2).shuffleGrouping("exclaim1");
        builder.setBolt("exclaim", new ExclamationBolt(), 2).shuffleGrouping("words");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
          // remote cluster
          conf.setNumWorkers(3);

          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
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
