package io.github.luzzu.qualityproblems;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.block.FileMode;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.StoreParams;
import org.apache.jena.tdb.setup.StoreParamsBuilder;
import org.apache.jena.tdb.sys.SystemTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.LuzzuFileLock;
import io.github.luzzu.operations.properties.PropertyManager;
import io.github.luzzu.semantics.commons.ResourceCommons;

public abstract class ProblemCollection<T>  {
	
	protected final static Logger logger = LoggerFactory.getLogger(ProblemCollection.class);


	public String TDB_DIRECTORY = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("TDB_TEMP_BASE_DIR")+"tdb_"+UUID.randomUUID().toString()+"/";
	
	protected Dataset dataset;
	protected String namedGraph;
	protected Resource metricURI;
	protected Resource problemURI;
	
	protected Location location;
	
	protected boolean isHPCEnabled = Boolean.parseBoolean(PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("HPC_COMPUTING"));
	protected LuzzuFileLock locker = LuzzuFileLock.getInstance();


	public ProblemCollection(Resource metricURI) {
		logger.debug("is HPC Enabled? {}", isHPCEnabled);
		
		if (isHPCEnabled) {
			logger.debug("Creating in-memory problem collection files");
			dataset = DatasetFactory.createTxnMem();
		} else {
			StoreParams custom64BitParams = StoreParamsBuilder.create()
					.fileMode(FileMode.mapped)
					.node2NodeIdCacheSize(10000000)
					.nodeId2NodeCacheSize(10000000)
					.build();
			
			location = Location.create(TDB_DIRECTORY);
			logger.info("TDB Location is {} for Metric {}", TDB_DIRECTORY, metricURI.getURI());
			
			logger.debug("Creating TDB problem collection files");
			if (SystemTDB.is64bitSystem) {
				TDBFactory.setup(location, custom64BitParams) ;
				dataset = TDBFactory.createDataset(location);
			} else {
				dataset = TDBFactory.createDataset(location);
			}
		}

		this.problemURI = ResourceCommons.generateURI(); // TODO: change to something more meaningful
		this.metricURI = metricURI;
		this.namedGraph = ResourceCommons.generateURI().toString();
	}

	public abstract void addProblem(T problematicElement);
	
	public abstract boolean isEmpty();
	
	public abstract void commit();
	
	
	/**
	 * Retrieves the graph name where the quality problem instances are stored
	 * @return Graph Name
	 */
	public String getNamedGraph() {
		return this.namedGraph;
	}
	
	/**
	 * Retrieves the TDB dataset with the quality problems for a metric
	 * @return TDB Dataset
	 */
	public Dataset getDataset() {
		commit();
		return dataset;
	}
	
	public void cleanup() {
		try {
			dataset.close();
		} catch (Exception e) { 
			logger.info("Dataset {} is already closed. Nothing to do here", TDB_DIRECTORY); 
		} 

		if (!isHPCEnabled) {
			try {
				getReentrantLock().lock();
				TDBFactory.release(dataset);
				//TODO: Clean up temp files if done
//				File f = new File(TDB_DIRECTORY);
//				FileUtils.deleteDirectory(f);
//			} catch (IOException e) {
//				e.printStackTrace();
			} finally {
				getReentrantLock().unlock();
			}
		}
		
		dataset = null;
		System.gc();
	}
	
	public Resource getProblemURI() {
		commit();
		return this.problemURI;
	}
	
	public synchronized ReentrantLock getReentrantLock() {
		return locker.getOrSetLockForFile(TDB_DIRECTORY);
	}
}


