package io.github.luzzu.operations.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;

/**
 * @author Jeremy Debattista
 *
 * The class Cache is a wrapper class for the LRUMap object
 * to enable the use of caching in the application.
 * 
 * The objects in this class are thread-safe.
 */
class Cache {

	private DB db;
	private String name;
	private HTreeMap<Object, CacheObject> cache;
	
	private Boolean serialise_cache = Boolean.parseBoolean(PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("SERIALISE_CACHE"));
	private String temp_location = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("CACHE_TEMP_BASE_DIR");

	final static Logger logger = LoggerFactory.getLogger(Cache.class);

	/**
	 * Initialise a new cache with an identifier name and the maximum amount of objects allowed.
	 * 
	 * @param name - Cache Identifier
	 * @param maxItems - Number of object allowed
	 */
	protected Cache(String name, int maxItems){
		this(name, maxItems, false);
	}
	
	@SuppressWarnings({ "unchecked"})
	protected Cache(String name, int maxItems, boolean forceHeapCache) {
		this.name = name;
		
		if (forceHeapCache) serialise_cache = false;
		
		File tempFolder = new File(temp_location); 
		if (!tempFolder.exists()) tempFolder.mkdirs();
		
		
		if (serialise_cache) {
			//TODO: serialised_cache=true is causing exceptions when doing multiple assessments!
			String tempFile = temp_location+"luzzu_"+name;
			
			this.db = DBMaker
					.fileDB(tempFile)
//					.fileDeleteAfterClose()
					.fileMmapEnableIfSupported()
					.fileMmapPreclearDisable()
					.cleanerHackEnable()
					.closeOnJvmShutdown()
					.closeOnJvmShutdownWeakReference()
					.fileChannelEnable()
					.make();
		} else {
			this.db = DBMaker
					.heapDB()
					.allocateStartSize(Integer.parseInt(PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("CACHE_SIZE_IN_GB")))
					.allocateIncrement(512)
					.make();
		}
		
		this.cache = (HTreeMap<Object, CacheObject>) db.hashMap("cache_"+name)
				.expireMaxSize(10000)
				.expireAfterGet(5, TimeUnit.DAYS)
				.createOrOpen();
	}
	
	/**
	 * Adds item to the cache.
	 * 
	 * @param key - An identifiable key for the item added
	 * @param value - The item added
	 */
	protected void addToCache(Object key, CacheObject value){
		this.cache.put(key, value);
	}
	
	/**
	 * Fetches item from the cache and marks it as the most recently used.
	 * 
	 * @param key - The identifiable key
	 * @return Returns the object from cache
	 */
	protected CacheObject getFromCache(Object key){
		return this.cache.get(key);
	}
	
	/**
	 * Checks if a particular object exists in the cache
	 * 
	 * @param key - The identifiable key
	 * @return Returns true if the object exists in the cache.
	 */
	protected boolean existsInCache(Object key){
		return this.cache.containsKey(key);
	}
	
	/**
	 * @return Returns the Cache Identifier Name
	 */
	protected String getName(){
		return this.name;
	}
	
	/**
	 * Clears memory from unused resources
	 */
	protected void cleanup(){
//		this.db.getEngine().clearCache();
		this.clear();
	}
	
	protected void clear(){
		this.db.commit();
		this.cache.clear();
//		this.db.compact();
	}
	
	/**
	 * Close all cache resources
	 */
	@Override
	protected void finalize() throws Throwable {
		try {
			this.cache.close();
			this.db.close();
		} catch(Throwable ex) {
			ExceptionOutput.output((Exception) ex, "Exception on Finalising Cache", logger);
		} finally {
			super.finalize();
		}
	}
}