package io.github.luzzu.communications;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import javax.json.stream.JsonGenerator;
import javax.net.ssl.SSLException;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;

public class Main {
	
	 private static boolean settings = false;
	
	// Base URI the Grizzly HTTP server will listen on
	private static Properties PROP = PropertyManager.getInstance().getProperties("luzzu.properties");
	private static String SCHEME = Boolean.parseBoolean(PROP.getProperty("ENABLE_SSH")) ? PROP.getProperty("SSLSCHEME") : PROP.getProperty("SCHEME");
	private static String DOMAIN = PROP.getProperty("DOMAIN");
	private static String PORT_NUMBER = PROP.getProperty("PORT");
	private static String APPLICATION = PROP.getProperty("APPLICATION");
	
	public static String BASE_URI = SCHEME+"://"+DOMAIN+":"+PORT_NUMBER+"/"+ APPLICATION + "/";
	
	final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static HttpServer startServer() {
        final ResourceConfig rc = new ResourceConfig().packages("io.github.luzzu").property(JsonGenerator.PRETTY_PRINTING, true);
       	HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
       	server.getHttpHandler().setAllowEncodedSlash(true);
       	server.getServerConfiguration().addHttpHandler(new StaticHttpHandler("src/main/webapp"), "/");
       	server.getServerConfiguration().addHttpHandler(new StaticHttpHandler("src/main/webapp"), "/assets/");
       	return server;
    }
    
    
    public static HttpServer startSSLServer() throws SSLException {
        final ResourceConfig rc = new ResourceConfig().packages("io.github.luzzu").property(JsonGenerator.PRETTY_PRINTING, true);
        
        SSLContextConfigurator sslCon = new SSLContextConfigurator();
        
        sslCon.setKeyStoreFile(PROP.getProperty("KEYSTORE_LOC"));
        sslCon.setKeyStorePass(PROP.getProperty("KEYSTORE_PASS"));

        sslCon.setTrustStoreFile(PROP.getProperty("TRUSTSTORE_LOC"));
        sslCon.setTrustStorePass(PROP.getProperty("TRUSTSTORE_PASS"));
        
        
        if (sslCon.validateConfiguration(true)) {
        	HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc, true, new SSLEngineConfigurator(sslCon).setClientMode(false).setNeedClientAuth(false));
           	
           	server.getHttpHandler().setAllowEncodedSlash(true);
           	server.getServerConfiguration().addHttpHandler(new StaticHttpHandler("src/main/webapp"), "/");
           	server.getServerConfiguration().addHttpHandler(new StaticHttpHandler("src/main/webapp"), "/assets/");
           	return server;
        } else {
        	throw new SSLException("SSL Configuration is not valid");
        }
    }
    
    private static void checkOrCreateDirectories() {
    	File localProp = new File("luzzu.properties");
    	if (!(localProp.exists()))
    		initialise();
    	
    	boolean changesMade = false;
    	String qmdFolder = PROP.getProperty("QUALITY_METADATA_BASE_DIR");
    	
    	String tdbTmpFolder = "";
    	if((PROP.getProperty("TDB_TEMP_BASE_DIR") == null) || (PROP.getProperty("TDB_TEMP_BASE_DIR").equals(""))) {
    		tdbTmpFolder = System.getProperty("java.io.tmpdir");
    		PROP.setProperty("TDB_TEMP_BASE_DIR", tdbTmpFolder);
    		System.out.println("Setting TDB Temp base dir to: " + PROP.getProperty("TDB_TEMP_BASE_DIR"));
    		changesMade = true;
    	}
    	tdbTmpFolder = PROP.getProperty("TDB_TEMP_BASE_DIR");
    	
    	String cacheTmpFolder = "";
    	if(PROP.getProperty("CACHE_TEMP_BASE_DIR") == null) {
    		cacheTmpFolder = System.getProperty("java.io.tmpdir");
    		PROP.setProperty("CACHE_TEMP_BASE_DIR", tdbTmpFolder);
    		System.out.println("Setting Cache Temp base dir to: " + PROP.getProperty("CACHE_TEMP_BASE_DIR"));
    		changesMade = true;
    	}
    	cacheTmpFolder = PROP.getProperty("TDB_TEMP_BASE_DIR");
    	
    	String tmpFolder = "";
    	if(PROP.getProperty("FILE_TEMP_BASE_DIR") == null) {
    		tmpFolder = System.getProperty("java.io.tmpdir");
    		PROP.setProperty("FILE_TEMP_BASE_DIR", tdbTmpFolder);
    		System.out.println("Setting Temp base dir to: " + PROP.getProperty("FILE_TEMP_BASE_DIR"));
    		changesMade = true;
    	}
    	tmpFolder = PROP.getProperty("TDB_TEMP_BASE_DIR");
    	
    	File folder = new File(qmdFolder);
		if (!(folder.exists())) {
			System.out.println("Creating Quality Metadata Folder");
			folder.mkdirs();
		}
		folder = new File(tdbTmpFolder);
		if (!(folder.exists())) {
			System.out.println("Creating TDB Temp Folder");
			folder.mkdirs();
    		changesMade = true;
		}
		folder = new File(cacheTmpFolder);
		if (!(folder.exists())) {
			System.out.println("Creating Cache Temp Folder");
			folder.mkdirs();
    		changesMade = true;
		}		
		folder = new File(tmpFolder);
		if (!(folder.exists())) {
			System.out.println("Creating File Temp Folder");
			folder.mkdirs();
    		changesMade = true;
		}
		
		if (changesMade) {
			try {
				PropertyManager.getInstance().store("luzzu.properties");
			} catch (IOException e) {
				ExceptionOutput.output(e, "[Luzzu Server] - Initialisation Error", logger);
			}
		}
    }

    private static void initialise() {
    	try {
			PropertyManager.getInstance().create("luzzu.properties");
		} catch (IOException e) {
			ExceptionOutput.output(e, "[Luzzu Server] - Initialisation Error", logger);
		}
    }
    
    private static void reloadWSsettings() {
    	SCHEME = Boolean.parseBoolean(PROP.getProperty("ENABLE_SSH")) ? PROP.getProperty("SSLSCHEME") : PROP.getProperty("SCHEME");
    	DOMAIN = PROP.getProperty("DOMAIN");
    	PORT_NUMBER = PROP.getProperty("PORT");
    	APPLICATION = PROP.getProperty("APPLICATION");
    	
    	BASE_URI = SCHEME+"://"+DOMAIN+":"+PORT_NUMBER+"/"+ APPLICATION + "/";
    }
    
    public static void main(String[] args) throws IOException {    	
    	if (args.length > 0) {
    		for (String arg : args) {
    			if (arg.equals("-settings")) {
    				settings = true;
    			}
    			if ((arg.equals("-help")) || (arg.equals("-help"))) {
    				System.out.println("Luzzu - A Quality Assessment Framework for Linked Data");
    				System.out.println("-settings : Displays the settings for the framework");
    				System.out.println("-help : Displays this help menu");
    				System.out.println("The server will start automatically and no argument is required.");
    			}
    		}
    	}
    	
    	checkOrCreateDirectories(); // Creates temporary directory for first run
    	
    	// If the settings argument is passed
    	if (settings) {
        	try {
    			PropertyManager.getInstance().edit("luzzu.properties");
    			System.out.println("Settings saved - loading Luzzu Server");
    			PROP = PropertyManager.getInstance().getProperties("luzzu.properties");
    			reloadWSsettings();
    		} catch (InterruptedException e) {
    			ExceptionOutput.output(e, "[Luzzu Server] - Error editing properties", logger);
    		}
    	}
    	
    	// Starting server
    	final HttpServer server = (Boolean.parseBoolean(PROP.getProperty("ENABLE_SSH")) == true) ? startSSLServer() : startServer();
    	try {
            server.start();
            System.out.println(String.format("Jersey app started with WADL available at " + "%sapplication.wadl\n", BASE_URI));
            System.out.println("Total Memory: "+ (double) Runtime.getRuntime().totalMemory() / (1024.0*1024.0) + " MB");
            System.out.println("Max Memory: "+ (double) Runtime.getRuntime().maxMemory() / (1024.0*1024.0) + " MB");
            System.out.println("Free Memory: "+ (double) Runtime.getRuntime().freeMemory() / (1024.0*1024.0) + " MB");
            
        	System.out.println("Log File: " + System.getProperty("java.io.tmpdir")+"/luzzu/debug.log");

            if (Boolean.parseBoolean(PROP.getProperty("WEB_APP_ENABLED")))
	    		if (java.awt.Desktop.isDesktopSupported()) 
	    			java.awt.Desktop.getDesktop().browse(new URI("http://localhost:8080"));

            // Wait forever (i.e. until the JVM instance is terminated externally)
            Thread.currentThread().join();
        } catch (Exception ioe) {
            System.out.println("Error running Luzzu Communications service: " + ioe.toString());
        } finally {
	    	if(server != null && server.isStarted()) {
	    		server.shutdownNow();
	    	}
        }
    }

}

