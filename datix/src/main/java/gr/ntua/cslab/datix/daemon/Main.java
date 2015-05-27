package gr.ntua.cslab.datix.daemon;

import gr.ntua.cslab.datix.daemon.cache.KDtreeCache;
import gr.ntua.cslab.datix.daemon.cache.MappingCache;
import gr.ntua.cslab.datix.daemon.shared.ServerStaticComponents;

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

/**
 * Executor class, used as an endpoint to the jar package from the outside
 * world.
 *
 * @author Dimitris Sarlis
 */
public class Main {

    private static Properties properties;
	private static Server server;

	private static void loadProperties() throws IOException {
        InputStream stream = Main.class.getClassLoader().getResourceAsStream("datix.properties");
        if (stream == null) {
            System.err.println("No datix.properties file was found! Exiting...");
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
                "gr.ntua.cslab.datix.daemon.rest;"
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
    
    private static void initialize() throws IOException, ClassNotFoundException {
    	ServerStaticComponents.setTableName("sflows_with_tree_partitioned");
    	ServerStaticComponents.setSlaves(new String[]{"slave1", "slave1", "slave2", "slave3", "slave4", "slave5", "slave6", "slave7", "slave8",
    																						  "slave9", "slave10", "slave11", "slave12", "slave13", "slave14" });
    	ServerStaticComponents.setLock(new ReentrantLock());
    	ServerStaticComponents.setKdTreeFile("/opt/kdtree/tree_partition_3D");
    	ServerStaticComponents.setMappingFile("/opt/mapping/mappingFile");
    	
    	File treeDir = new File("/opt/kdtree/");
    	File mappingDir = new File("/opt/mapping/");
    	
    	if (!treeDir.exists() && !mappingDir.exists()) {
    		treeDir.mkdir();
    		mappingDir.mkdir();
    		KDtreeCache.setKd(new KdTree<Long>(KDtreeCache.getDimensions().length, KDtreeCache.getBucketSize()));
    		MappingCache.setFileMapping(new HashMap <String, String>());
    		MappingCache.updateMapping("1", "slave1");
    	}
    	else {
    		BufferedReader br = new BufferedReader(new FileReader(ServerStaticComponents.getKdTreeFile()));
    		KDtreeCache.setKd(new KdTree<Long>(br));
//    		ObjectInputStream s = new ObjectInputStream(new FileInputStream(ServerStaticComponents.getMappingFile()));
//    		MappingCache.setFileMapping((HashMap<String, String>) s.readObject());
//    		s.close();
    		MappingCache.setFileMapping(new HashMap <String, String>());
    		MappingCache.updateMapping("1", "slave1");
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

    }
}
