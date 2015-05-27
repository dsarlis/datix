package gr.ntua.cslab.datixSlave.daemon;

import gr.ntua.cslab.datixSlave.beans.SflowsList;
import gr.ntua.cslab.datixSlave.daemon.Main;
import gr.ntua.cslab.datixSlave.daemon.cache.*;
import gr.ntua.cslab.datixSlave.daemon.shared.SlaveStaticComponents;
import gr.ntua.cslab.datixSlave.daemon.threads.ZookeeperThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import ags.utils.dataStructures.trees.thirdGenKD.KdTree;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class Main {
	
	private static Properties properties;
	private static Server server;

	private static void loadProperties() throws IOException {
        InputStream stream = Main.class.getClassLoader().getResourceAsStream("datix-slave.properties");
        if (stream == null) {
            System.err.println("No datix-slave.properties file was found! Exiting...");
            System.exit(1);
        }
       properties = new Properties();
        properties.load(stream);
    }

    private static void configureServer() throws Exception {
        server = new Server();
        int plainPort = -1, sslPort = -1;
        String keystorePath = null, keystorePassword = null;
        ServerConnector connector = null, sslConnector = null;

        if (properties.getProperty("server.plain.port") != null) {
            plainPort = new Integer(properties.getProperty("server.plain.port"));
        }
        if (properties.getProperty("server.ssl.port") != null) {
            sslPort = new Integer(properties.getProperty("server.ssl.port"));
            keystorePath = properties.getProperty("server.ssl.keystore.path");
            keystorePassword = properties.getProperty("server.ssl.keystore.password");
        }

        if (plainPort != -1) {
            connector = new ServerConnector(server);
            connector.setPort(plainPort);
        }

        if (sslPort != -1) {
            HttpConfiguration https = new HttpConfiguration();
            https.addCustomizer(new SecureRequestCustomizer());
            SslContextFactory sslContextFactory = new SslContextFactory();
            sslContextFactory.setKeyStorePath(keystorePath);
            sslContextFactory.setKeyStorePassword(keystorePassword);
            sslContextFactory.setKeyManagerPassword(keystorePassword);

            sslConnector = new ServerConnector(
                    server,
                    new SslConnectionFactory(sslContextFactory, "http/1.1"),
                    new HttpConnectionFactory(https));
            sslConnector.setPort(sslPort);

        }
        if (sslConnector != null && connector != null) {
            server.setConnectors(new Connector[]{connector, sslConnector});
        } else if (connector != null) {
            server.setConnectors(new Connector[]{connector});
        } else if (sslConnector != null) {
            server.setConnectors(new Connector[]{sslConnector});
        } else {
            System.err.println("Please choose one of the plain and SSL connections!");
            System.exit(1);
        }

        ServletHolder holder = new ServletHolder(ServletContainer.class);
        holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig");
        holder.setInitParameter("com.sun.jersey.config.property.packages",
                "gr.ntua.cslab.datixSlave.daemon.rest;"
                + "org.codehaus.jackson.jaxrs");//Set the package where the services reside
        holder.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true");
        holder.setInitParameter("com.sun.jersey.config.feature.Formatted", "true");
//        
        holder.setInitOrder(1);
//
//        ServerStaticComponents.server = new Server();
        ServletContextHandler context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        context.addServlet(holder, "/*");
        Logger.getLogger(Main.class.getName()).info("Server configured");

    }

    private static void configureLogger() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        InputStream logPropertiesStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
        PropertyConfigurator.configure(logPropertiesStream);
        Logger.getLogger(Main.class.getName()).info("Logger configured");
    }

    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Logger.getLogger(Main.class.getName()).info("Server is shutting down");
                    server.stop();
                } catch (Exception ex) {
                    Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }));

    }
    
    private static void initialize() throws IOException {
    	
    	SFlowsCache.setSflowsToStore(new HashMap<String, SflowsList>());
    	SFlowsCache.setCachedSflows(new HashMap<String, SflowsList>());
    	SlaveStaticComponents.setTableName("sflows_with_tree_partitioned");
    	SlaveStaticComponents.setKdTreeFile("/opt/kdtree/tree_partition_3D");
    	SlaveStaticComponents.setMappingFile("/opt/mapping/mappingFile");
    	SlaveStaticComponents.setLock(new ReentrantLock());
    	SlaveStaticComponents.setStoreLock(new ReentrantLock());
    	SlaveStaticComponents.setLocked(false);
    	
    	File treeDir = new File("/opt/kdtree/");
    	File mappingDir = new File("/opt/mapping/");
    	
    	if (!treeDir.exists()) {
    		treeDir.mkdir();
    		mappingDir.mkdir();
    		KDtreeCache.setKd(new KdTree<Long>(KDtreeCache.getDimensions().length, KDtreeCache.getBucketSize()));
    		MappingCache.setFileMapping(new HashMap <String, String>());
    		MappingCache.updateMapping("1", "slave1");
    	}
    	else {
    		BufferedReader br = new BufferedReader(new FileReader(SlaveStaticComponents.getKdTreeFile()));
    		KDtreeCache.setKd(new KdTree<Long>(br));
    	}
    }

    public static void main(String[] args) throws Exception {
        configureLogger();
        loadProperties();
        addShutdownHook();
        configureServer();
        initialize();

        server.start();
        Logger.getLogger(Main.class.getName()).info("Server is started");
        Thread rz = new Thread(new ZookeeperThread("master:2181", "/datix"));
        rz.start();
        Thread isAlive = new Thread(new ZookeeperThread("master:2181", "/live"));
        isAlive.start();
    }
}
