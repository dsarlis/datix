package gr.ntua.cslab.streamnet;

import gr.ntua.cslab.streamnet.bolts.SFlowBolt;

import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.bridge.SLF4JBridgeHandler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {
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
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new SFlowBolt("test"), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new SFlowBolt("test"), 2).shuffleGrouping("exclaim1");

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
