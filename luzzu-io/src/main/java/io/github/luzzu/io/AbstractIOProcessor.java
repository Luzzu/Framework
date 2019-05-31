package io.github.luzzu.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;


import io.github.luzzu.annotations.ProblemReport;
import io.github.luzzu.annotations.QualityMetadata;
import io.github.luzzu.assessment.ComplexQualityMetric;
import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.assessment.internalmetrics.CountMetric;
import io.github.luzzu.assessment.internalmetrics.SyntaxErrorMetric;
import io.github.luzzu.datatypes.Args;
import io.github.luzzu.exceptions.AfterException;
import io.github.luzzu.exceptions.BeforeException;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.MetadataException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.exceptions.SyntaxErrorException;
import io.github.luzzu.io.configuration.DeclerativeMetricCompiler;
import io.github.luzzu.io.configuration.ExternalMetricLoader;
import io.github.luzzu.io.helper.IOStats;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.operations.cache.CacheManager;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.lowlevel.LuzzuFileLock;
import io.github.luzzu.operations.properties.PropertyManager;
import io.github.luzzu.qml.parser.ParseException;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.vocabularies.LMI;
import io.github.luzzu.io.LoadToDataStore;

public abstract class AbstractIOProcessor implements IOProcessor {

	// Variables for dataset assessment
	protected String datasetPLD;
	protected String datasetLocation;
	protected boolean genQualityReport;
	protected Model metricConfiguration;
	protected String crawlDate = null;
	
	// Variables for metric threads
	protected boolean isInitalised = false;
	protected List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();
	protected ConcurrentMap<String, QualityMetric<?>> metricInstances = new ConcurrentHashMap<String, QualityMetric<?>>();
	protected ExecutorService executor;	
	
	// Variables for metric loader in processor
	protected ExternalMetricLoader loader = ExternalMetricLoader.getInstance();
	protected DeclerativeMetricCompiler dmc  = DeclerativeMetricCompiler.getInstance(); // Loads LQML instances

	// Variables for processor
	protected final CacheManager cacheMgr = CacheManager.getInstance();
	protected final String graphCacheName = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("GRAPH_METADATA_CACHE");
	protected final String metadataBaseDir;
	
	// Variables for Statistics
	private boolean isGeneratingQMD = false;
	private boolean endedGeneratingQMD = false;
	
	private boolean isGeneratingQR = false;
	private boolean endedGeneratingQR = false;
	
	// cancel processing
	protected boolean forcedCancel = false;
	
	protected AtomicBoolean signalSyntaxError = new AtomicBoolean(false);
	
	protected CountMetric countMetric = new CountMetric();
	
	protected LuzzuFileLock locker = LuzzuFileLock.getInstance();
	
	//Triple Store Upload
	protected boolean useFusekiStore = false;
	
	protected Logger logger = null;
	
	protected Map<String, Resource> observationURIs = new ConcurrentHashMap<String, Resource>();
	
	{	
		// Initialize cache manager
		cacheMgr.createNewCache(graphCacheName, 50, true);
		
		// Load properties from configuration files
		PropertyManager props = PropertyManager.getInstance();
		// If the directory to store quality metadata and problem reports was not specified, set it to user's home
		if(props.getProperties("luzzu.properties") == null || 
				props.getProperties("luzzu.properties").getProperty("QUALITY_METADATA_BASE_DIR") == null) {
			metadataBaseDir = System.getProperty("user.dir") + "/qualityMetadata";
		} else {
			metadataBaseDir = props.getProperties("luzzu.properties").getProperty("QUALITY_METADATA_BASE_DIR");
			useFusekiStore = Boolean.parseBoolean(props.getProperties("luzzu.properties").getProperty("USE_FUSEKI_SERVER"));
		}
	}
	
	public AbstractIOProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration) {
		this.datasetLocation = datasetLocation;
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		this.datasetPLD = datasetPLD;
		
		PropertyManager.getInstance().addToEnvironmentVars("dataset-pld", this.datasetPLD);
		PropertyManager.getInstance().addToEnvironmentVars("dataset-location", this.datasetLocation);
		PropertyManager.getInstance().addToEnvironmentVars("require-quality-report", Boolean.toString(genQualityReport));

	}
		
	public AbstractIOProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		this(datasetPLD,datasetLocation,genQualityReport,configuration);
		this.crawlDate = crawlDate;
	}
	
	
	
	// -- Implemented Classes -- //
	public void processorWorkFlow() throws LuzzuIOException, InterruptedException {
		try {
			this.setUpProcess();
			
			startProcessing();
			
			SyntaxErrorMetric metric = new SyntaxErrorMetric();
			metric.setHasErrors(signalSyntaxError.get());
			this.metricInstances.put("SyntaxErrorMetric", metric);
			this.metricInstances.put("CountMetric", this.countMetric);

			executor.shutdown();
			if (!forcedCancel){
				this.generateAndWriteQualityMetadataReport();
				if (this.genQualityReport) {
					this.generateAndWriteQualityProblemReport();
				} else {
					this.clearTDBFiles();
				}
			} else if (signalSyntaxError.get()) {
				this.metricInstances.clear();
				this.metricInstances.put("SyntaxErrorMetric", metric);
				this.generateAndWriteQualityMetadataReport();
			} else {
				this.clearTDBFiles();
			}
		} catch (SyntaxErrorException e) {
			SyntaxErrorMetric metric = new SyntaxErrorMetric();
			metric.setHasErrors(signalSyntaxError.get());
			this.metricInstances.clear();
			this.metricInstances.put("SyntaxErrorMetric", metric);
			this.generateAndWriteQualityMetadataReport();
			
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}
	
	public void cleanUp() throws LuzzuIOException {
		this.isInitalised = false;
		
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
				
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
	}
	
	@Override
	public synchronized List<IOStats> getIOStats() throws ProcessorNotInitialised {
		List<IOStats> lst = new ArrayList<IOStats>();
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Processor has not been initalised, therefore there are no statistics");	
		
		for (MetricProcess mp : lstMetricConsumers){
			Long stmtProcessed = mp.getStatementProcessed();
			String metricName = mp.getMetricName();
			boolean doneParsing = mp.isDoneParsing();
			
			IOStats ios = new IOStats(metricName,stmtProcessed, doneParsing);
			
			if ((this.isGeneratingQMD) && (!this.endedGeneratingQMD))
				ios.setQmdStatus("Generating Quality Metadata");
			if ((!this.isGeneratingQMD) && (this.endedGeneratingQMD))
				ios.setQmdStatus("Finished Generating Quality Metadata");
			
			if ((this.isGeneratingQR) && (!this.endedGeneratingQR))
				ios.setQmdStatus("Generating Quality Problem Report");
			if ((!this.isGeneratingQR) && (this.endedGeneratingQR))
				ios.setQmdStatus("Finished Generating Quality Problem Report");
			
			lst.add(ios);
		}
		
		return lst;
	}
	
	
	// -- Protected Classes -- //
	protected void loadMetrics() throws ExternalMetricLoaderException {
		NodeIterator iter = metricConfiguration.listObjectsOfProperty(LMI.metric);
		if (!(iter.hasNext())){
			throw new ExternalMetricLoaderException("Model Cannot be Empty");
		}
		Map<String, Class<? extends QualityMetric<?>>> map = loader.getQualityMetricClasses();
		
		try {
			map.putAll(this.dmc.compile());
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		while(iter.hasNext()){
			String className = iter.next().toString();
			Class<? extends QualityMetric<?>> clazz = map.get(className);
			QualityMetric<?> metric = null;
			try {
				metric = clazz.newInstance();
				metric.setDatasetURI(this.datasetPLD);
			} catch (InstantiationException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance for " + className + ". Exception caused by an Instantiation Exception : " + e.getLocalizedMessage());
			} catch (IllegalAccessException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance " + className + ". Exception caused by an Illegal Access Exception : " + e.getLocalizedMessage());
			} catch (NullPointerException e){
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance " + className + ". Exception caused by an Illegal Access Exception : " + e.getLocalizedMessage());
			} 
			metricInstances.put(className, metric);
		}
		
		for(String className : this.metricInstances.keySet()) {
			if (this.metricInstances.get(className) instanceof ComplexQualityMetric<?>){
				try {
					List<Args> args = loader.getBeforeArgs(className);
				
					List<Object> pass = new ArrayList<Object>();
					for(Args arg : args){
						pass.add(this.transformJavaArgs(Class.forName(arg.getType()), arg.getParameter()));
					}

					((ComplexQualityMetric<?>)this.metricInstances.get(className)).before(pass.toArray());
				} catch (BeforeException | ClassNotFoundException  e) {
					logger.error(e.getMessage());
				}
			}
			this.lstMetricConsumers.add(new MetricProcess(logger, this.metricInstances.get(className)));
		}
	}
	
	protected Object transformJavaArgs(Class<?> target, String s){
		 if (target == Object.class || target == String.class || s == null) {
		        return s;
		    }
		    if (target == Character.class || target == char.class) {
		        return s.charAt(0);
		    }
		    if (target == Byte.class || target == byte.class) {
		        return Byte.parseByte(s);
		    }
		    if (target == Short.class || target == short.class) {
		        return Short.parseShort(s);
		    }
		    if (target == Integer.class || target == int.class) {
		        return Integer.parseInt(s);
		    }
		    if (target == Long.class || target == long.class) {
		        return Long.parseLong(s);
		    }
		    if (target == Float.class || target == float.class) {
		        return Float.parseFloat(s);
		    }
		    if (target == Double.class || target == double.class) {
		        return Double.parseDouble(s);
		    }
		    if (target == Boolean.class || target == boolean.class) {
		        return Boolean.parseBoolean(s);
		    }
		    throw new IllegalArgumentException("Don't know how to convert to " + target);
	}
	
	protected void computeAfter() {
		for(String clazz : metricInstances.keySet()) {
			if(metricInstances.get(clazz) instanceof ComplexQualityMetric<?>) {
				try {
					List<Args> args = loader.getBeforeArgs(clazz);
					
					List<Object> pass = new ArrayList<Object>();
					for(Args arg : args) {
						pass.add(transformJavaArgs(Class.forName(arg.getType()), arg.getParameter()));
					}

					((ComplexQualityMetric<?>)this.metricInstances.get(clazz)).after(pass.toArray());				
				} catch (AfterException | ClassNotFoundException  e) {
					logger.error(e.getMessage());
				}
			}
		}
	}
	
	/**
	 * Generates the quality problem report associated to this quality assessment process. 
	 */
	protected synchronized void generateAndWriteQualityProblemReport() {
		this.isGeneratingQR = true;
		File prFile = prepareFileForProblemReport();
		
		Resource datasetPLDResource = ModelFactory.createDefaultModel().createResource(this.datasetPLD);

		ProblemReport report;
		try {
			report = new ProblemReport(datasetPLDResource, prFile);
			
			for(String className : this.metricInstances.keySet()){
				QualityMetric<?> metric = this.metricInstances.get(className);
				
				ProblemCollection<?> problemCollection = metric.getProblemCollection();
				if (problemCollection != null) {
					if (!problemCollection.isEmpty())  {
						report.addToQualityProblemReport(problemCollection, observationURIs.get(className));
					}
				}
			}
			
			report.closeSerialisedFile();
			String date = (new io.github.luzzu.operations.lowlevel.Date()).getDate();

			logger.debug("[IOProcessor - {}] Quality problem report for {} written successfully. File stored: {}", date, this.datasetPLD, prFile.getPath());
			
			this.isGeneratingQR = false;
			this.endedGeneratingQR = true;
			
//			report.flush();
			report.cleanup();
		} catch (IOException e) {
			ExceptionOutput.output(e, "Error in generating Problem Report File for "+this.datasetPLD, logger);
		}
		
		//Write ProblemReport to Datastore
		String fileName = prFile.getName();
		int lastIndexPos = fileName.lastIndexOf(".");
		if (lastIndexPos > 0) {
			fileName = fileName.substring(0, lastIndexPos);
		}
		if(useFusekiStore)
		{
		//Write ProblemReport to Datastore
		LoadToDataStore ltds = new LoadToDataStore();
		ltds.loadData(fileName,prFile.getAbsolutePath());
		}

	}
	
	protected void clearTDBFiles() {
		for(String className : this.metricInstances.keySet()){
			QualityMetric<?> metric = this.metricInstances.get(className);
			ProblemCollection<?> problemCollection = metric.getProblemCollection();
			if (problemCollection != null) 
				problemCollection.cleanup();
		}
	}
		

	/**
	 * Generates the quality metadata corresponding to the data processed by this instance. Stores the 
	 * resulting metadata into a file, along the corresponding configuration parameters.
	 */
	protected synchronized void generateAndWriteQualityMetadataReport() {
		this.isGeneratingQMD = true;
		this.createSerialisationLocation();

		String fld = "";
		if (this.datasetPLD.contains("http://"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("http://", "");
		else if (this.datasetPLD.contains("https://"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("https://", "");
		else if (this.datasetPLD.contains("uri:"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("uri:", "");

		fld = fld.replaceFirst("^~",System.getProperty("user.home"));
				
		String metadataFilePath = fld + "/daq-metadata.trig";
		metadataFilePath = metadataFilePath.replace("//", "/");
		metadataFilePath = metadataFilePath.replace(":", "_");
		
		String date = (new io.github.luzzu.operations.lowlevel.Date()).getDate();

		logger.info("[IOProcessor - {}] Writing quality metadata report for {} to file: {}", date, this.datasetPLD, metadataFilePath);
		
		ReentrantLock lock = locker.getOrSetLockForFile(metadataFilePath);
		
		try {
			lock.lock();
			File fileMetadata = new File(metadataFilePath);
			Dataset model = DatasetFactory.create();

			if(fileMetadata.exists()) {
				try { 
					RDFDataMgr.read(model, metadataFilePath);//, this.datasetPLD, Lang.TURTLE);
				} catch (Exception e) {
					ExceptionOutput.output(e, "Error in reading previous quality metadata for "+this.datasetPLD, logger);
				}
			}
			
			// Note that createResource() intelligently reuses the resource if found within a read model
			Resource res = ModelFactory.createDefaultModel().createResource(this.datasetPLD);
			
			QualityMetadata md;
			if (this.crawlDate == null) md = new QualityMetadata(model, res);
			else md = new QualityMetadata(model, res, this.crawlDate);
			
			
			// Write quality metadata about the metrics assessed through this processor
			for(String className : this.metricInstances.keySet()){
				QualityMetric<?> m = this.metricInstances.get(className);
				try { 
					Resource observation = md.addMetricData(m);
					observationURIs.put(className, observation);
				} catch (MetadataException e) {
					ExceptionOutput.output(e, "Error in creating quality metadata", logger);
				}
			}

			try {
				// Make sure the file is created (the following call has no effect if the file exists)
				fileMetadata.createNewFile();
				// Write new quality metadata into file
				OutputStream out = new FileOutputStream(fileMetadata, false);
				RDFDataMgr.write(out, md.createQualityMetadata(), RDFFormat.TRIG_PRETTY);
				
				logger.info("[IOProcessor - {}] Quality metadata for {} written successfully. File stored: {}",this.datasetPLD,metadataFilePath);
				if(useFusekiStore)
				{
				//Write Quality Metadata to Datastore
				LoadToDataStore ltds = new LoadToDataStore();
				ltds.loadData("defaultGraph",metadataFilePath);
				}
			} catch(MetadataException | IOException ex) {
				ExceptionOutput.output(ex, "Error in generating quality metadata file for "+this.datasetPLD, logger);
			}
			
			this.isGeneratingQMD = false;
			this.endedGeneratingQMD = true;
		} finally {
			lock.unlock();
		}
	}
	
	
	private synchronized File prepareFileForProblemReport() {
		this.createSerialisationLocation();

		long timestamp = (new Date()).getTime();
		String metadataFilePath = "";
		
		if (this.datasetPLD.contains("http://"))
			metadataFilePath = String.format("%s/%s/problem-report-%d.ttl", this.getMetadataBaseDir(), this.datasetPLD.replace("http://", ""), timestamp);
		else if (this.datasetPLD.contains("https://"))
			metadataFilePath = String.format("%s/%s/problem-report-%d.ttl", this.getMetadataBaseDir(), this.datasetPLD.replace("https://", ""), timestamp);
		else if (this.datasetPLD.contains("uri:"))
			metadataFilePath = String.format("%s/%s/problem-report-%d.ttl", this.getMetadataBaseDir(), this.datasetPLD.replace("uri:", ""), timestamp);

				
		metadataFilePath = metadataFilePath.replace("//", "/");
		metadataFilePath = metadataFilePath.replaceFirst("^~",System.getProperty("user.home"));
		metadataFilePath = metadataFilePath.replace(":", "_");

		String date = (new io.github.luzzu.operations.lowlevel.Date()).getDate();

		logger.info("[IOProcessor - {}] Writing quality problem report for {} to file: {}", date, this.datasetPLD, metadataFilePath);
		
		File fileMetadata = new File(metadataFilePath);
		return fileMetadata;
	}
	
	private void createSerialisationLocation() {
		String fld = "";
		if (this.datasetPLD.contains("http://"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("http://", "");
		else if (this.datasetPLD.contains("https://"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("https://", "");
		else if (this.datasetPLD.contains("uri:"))
			fld = this.getMetadataBaseDir() + "/" + this.datasetPLD.replace("uri:", "");
		
		fld = fld.replaceFirst("^~",System.getProperty("user.home"));
		fld = fld.replace(":", "_");

				
		File folder = new File(fld);
		if (!(folder.exists())) folder.mkdirs();
	}

	// -- Getters -- //
	public String getDatasetPLD() {
		return datasetPLD;
	}

	public String getDatasetLocation() {
		return datasetLocation;
	}

	public boolean isGenQualityReport() {
		return genQualityReport;
	}

	public Model getMetricConfiguration() {
		return metricConfiguration;
	}

	public String getCrawlDate() {
		return crawlDate;
	}

	public String getMetadataBaseDir() {
		return metadataBaseDir;
	}
	
	public int getNumberOfInitMetrics() {
		return this.metricInstances.size();
	}
	
	// -- Abstract Classes that need to be implemented -- //
	public abstract void startProcessing() throws LuzzuIOException, InterruptedException;
	
	public abstract void setUpProcess() throws LuzzuIOException;
	
	public abstract void cancelMetricAssessment() throws LuzzuIOException;

}
