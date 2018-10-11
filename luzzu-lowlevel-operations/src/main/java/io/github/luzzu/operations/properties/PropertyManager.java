package io.github.luzzu.operations.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;

/**
 * @author Jeremy Debattista
 * 
 * Handles the loading and storing of the defined 
 * properties found in src/main/resources/properties 
 * folder.
 */
public class PropertyManager {

	final static Logger logger = LoggerFactory.getLogger(PropertyManager.class);
	
	private static PropertyManager instance = null;
	private ConcurrentMap<String,Properties> propMap = new ConcurrentHashMap<String,Properties>();
	protected ConcurrentMap<String, String> environmentVars = new ConcurrentHashMap<String, String>();	
	
	protected PropertyManager(){
		try{
			Properties prop = new Properties();
			
			File localProperties = new File("luzzu.properties");
			if (localProperties.exists())
				prop.load(new FileInputStream(localProperties));
			else 
				prop.load(getClass().getResourceAsStream("/properties/luzzu.properties"));
			propMap.put("luzzu.properties", prop);	
		} catch (IOException ex)  {
			ExceptionOutput.output(ex, "[Property Manager] - Cannot load property file", logger);
		}
	}
	
	/**
	 * Create or return a singleton instance of the PropertyManager
	 * 
	 * @return PropertyManager instance
	 */
	public static PropertyManager getInstance(){
		if (instance == null) instance = new PropertyManager();
		return instance;
	}
	
	/**
	 * Returns the properties of a particular configuration.
	 * @param propertiesRequired - The file name (e.g. luzzu.properties) for the requried configuration.
	 * @return The properties for a configuration.
	 */
	public Properties getProperties(String propertiesRequired){
		return this.propMap.get(propertiesRequired);
	}
	
	/**
	 * Adds an environment variable value
	 * 
	 * @param key - Variable's name
	 * @param value - Variable's value
	 */
	public void addToEnvironmentVars(String key, String value){
		this.environmentVars.put(key, value);
	}
	
	/**
	 * Create or return a singleton instance of the PropertyManager
	 * 
	 * @return PropertyManager instance
	 * @throws IOException 
	 */
	public void store(String propertiesFile) throws IOException{
		if (instance == null) instance = new PropertyManager();
		
		File propFile = new File(propertiesFile);
		
		if (!propFile.exists()) {
			create(propertiesFile);
		}
		
		OutputStream os = new FileOutputStream(propFile);
		instance.getProperties(propertiesFile).store(os, "Luzzu Properties");
		
		instance = new PropertyManager(); // reload
	}
	
	public void edit(String propertiesFile) throws IOException, InterruptedException{
		if (instance == null) instance = new PropertyManager();
		
		File propFile = new File(propertiesFile);
		
		if (!propFile.exists()) {
			create(propertiesFile);
		}
		
//		if (java.awt.Desktop.isDesktopSupported()) java.awt.Desktop.getDesktop().edit(propFile);
		ProcessBuilder builder = new ProcessBuilder("vi", propFile.getPath());
		builder.inheritIO();
		builder.start().waitFor();
		
		instance = new PropertyManager(); // reload
	}
	
	public void create(String propertiesFile) throws IOException {
		if (instance == null) instance = new PropertyManager();
		
		IOUtils.copy(getClass().getResourceAsStream("/properties/luzzu.properties"), new FileOutputStream(new File("luzzu.properties")));
	}
}