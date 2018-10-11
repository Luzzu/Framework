package io.github.luzzu.io.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;

// Expects a .nt.gz file as input
public class LargeNTGZStreamProcessor extends AbstractIOProcessor {

	final static Logger logger = LoggerFactory.getLogger(LargeNTGZStreamProcessor.class);
	
	// RDFIterator parameters
	private final int rdfIterBufferSize = PipedRDFIterator.DEFAULT_BUFFER_SIZE * 2;
	private final int rdfIterPollTimeout = 10000000;
	private final int rdfIterMaxPolls = 50000;
	private final boolean rdfIterFairBufferLock = true;
	protected ConcurrentLinkedQueue<PipedRDFStream<?>> rdfStreamQueue = new ConcurrentLinkedQueue<PipedRDFStream<?>>(); 
	protected List<PipedRDFStream<?>> rdfStreamList = new ArrayList<PipedRDFStream<?>>();
	protected List<PipedRDFIterator<?>> iteratorList = new ArrayList<PipedRDFIterator<?>>();

	private String scriptPath = "";

	public LargeNTGZStreamProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration){
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = LargeNTGZStreamProcessor.logger;
		
		logger.warn("Creating NTGZ content reader");
		this.scriptPath = this.createScriptFile();
		logger.warn("Temporary Script File created: "+this.scriptPath);

	}
	
	public LargeNTGZStreamProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = LargeNTGZStreamProcessor.logger;
		
		logger.warn("Creating NTGZ content reader");
		this.scriptPath = this.createScriptFile();
		logger.warn("Temporary Script File created: "+this.scriptPath);
	}
	
	
	@SuppressWarnings("unchecked")
	public void setUpProcess() throws LuzzuIOException {
			Lang lang  = RDFLanguages.filenameToLang(this.datasetLocation);
			
			final int cores = Runtime.getRuntime().availableProcessors();
			logger.warn("Number of cores: {}",cores);
	
			if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
				for (int i = 0; i <= cores; i++) {
					PipedRDFIterator<?> iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
					iteratorList.add(iterator);
					PipedRDFStream<?> pipedStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
					rdfStreamQueue.add(pipedStream);
					rdfStreamList.add(pipedStream);
				}
			} else {
				for (int i = 0; i <= cores; i++) {
					PipedRDFIterator<?> iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
					iteratorList.add(iterator);
					PipedRDFStream<?> pipedStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
					rdfStreamQueue.add(pipedStream);
					rdfStreamList.add(pipedStream);
				}
			}
			logger.debug("PipedRDFIterator initialized with: Buffer Size {}, Fair Lock {}, Poll Timeout {}, Max Polls {}", 
					rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			
			try {
				loadMetrics();
				this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				this.isInitalised = true;
			} catch (ExternalMetricLoaderException e) {
				ExceptionOutput.output(e, "Error parsing dataset: "+ datasetLocation, logger);
			}
	}

	
	private Long totalNumberTriples() throws IOException{
		String[] command = {
				"gunzip",
				"-c",
				this.datasetLocation.replace("file:", "")
			};
			
	    Process process;
		try {
			process = Runtime.getRuntime().exec(command);
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));   
			LineNumberReader lnr = new LineNumberReader(reader);
			lnr.skip(Long.MAX_VALUE);
			long totalNumberOfLines = lnr.getLineNumber();
			return totalNumberOfLines;
		} catch (IOException e) {
			ExceptionOutput.output(e, "IO Exception thrown on Large Stream Processor: "+ datasetLocation, logger);
		} 
		return null;
	}
	
	private String createScriptFile() {
		String fileloc = "";
		URL path = LargeNTGZStreamProcessor.class.getClassLoader().getResource("viewcontents.sh");
		try {
			String tmpLoc = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("FILE_TEMP_BASE_DIR");
			File tempFile = File.createTempFile("viewcontents", ".sh", new File(tmpLoc));
			FileUtils.copyURLToFile(path, tempFile);
			fileloc = tempFile.getAbsolutePath();
		} catch (IOException e) {
			ExceptionOutput.output(e, "IO Exception thrown on Large Stream Processor: "+ datasetLocation, logger);
		}
		return fileloc;
	}
	
	public InputStream getPartialSubset(long min, long max){
//		String scriptPath = LargeNTGZStreamProcessor.class.getClassLoader().getResource("viewcontents.sh").getPath();
		
		String[] setPerm = {
				"chmod",
				"+x",
				this.scriptPath
		};
		
	    try {
			@SuppressWarnings("unused")
			Process setPermissions = Runtime.getRuntime().exec(setPerm);
		} catch (IOException e1) {
			ExceptionOutput.output(e1, "IO Exception thrown on Large Stream Processor: "+ datasetLocation, logger);
		}
		
		String[] command = {
				this.scriptPath,
				this.datasetLocation.replaceAll("file:", ""),
				Long.toString(min),
				Long.toString(max)
			};
		
	    Process process;
		try {
			process = Runtime.getRuntime().exec(command);
			InputStream reader =  process.getInputStream();   
			return reader;
		} catch (IOException e) {
			ExceptionOutput.output(e, "IO Exception thrown on Large Stream Processor: "+ datasetLocation, logger);
		} 
		return null;
	}

	public void startProcessing() throws LuzzuIOException, InterruptedException {
				
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		//Splitting large file 
		final int cores = Runtime.getRuntime().availableProcessors();
		long totalTriples = 0;
		try {
			totalTriples = totalNumberTriples();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		final long splitter = (long)Math.ceil(totalTriples / cores);
		logger.warn("Number of triples {} per core: {}",totalTriples, splitter);

		
		final Lang lang  = RDFLanguages.filenameToLang(this.datasetLocation);

	    long nextParsingCount = 0l;
	    int coreCounter = 1;
	    
	    do {
	    		final long edgeCount = ((nextParsingCount + splitter) > totalTriples) ? totalTriples : (nextParsingCount + splitter);
	    		final long startCount = nextParsingCount + 1l;
	    		logger.warn("Core {} : {} - {}",coreCounter, startCount, edgeCount);

			Runnable parser = new Runnable() {
				public void run() {
					try{
						InputStream reader;
						reader = getPartialSubset(startCount, edgeCount);
						RDFDataMgr.parse(rdfStreamQueue.poll(), reader, lang);
					} catch (Exception e) {
//						System.out.println(startCount + " " + edgeCount);
//						logger.error("[Large Stream Processor - {}] Error parsing dataset {}. Error message {}", (new io.github.luzzu.luzzu.commons.Date()).getDate(), datasetLoc, e.getMessage());
						ExceptionOutput.output(e, "IO Exception thrown on Large Stream Processor: ", logger);
						signalSyntaxError.set(true);
						forcedCancel = true;
					}
				}
			};
			executor.submit(parser);
			
			coreCounter++;
			nextParsingCount = edgeCount;
	    } while(nextParsingCount < totalTriples);

		
		long totalQuadsProcessed = 0;
		final SharedValue<Long> triplesProcessed = new SharedValue<Long>(totalQuadsProcessed);

		try {
			ExecutorService iteratorExec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			for (final PipedRDFIterator<?> iterator : this.iteratorList) {
				Runnable iterThread = new Runnable() {
					public void run() {
						while (iterator.hasNext()) {
							Object2Quad stmt = new Object2Quad(iterator.next());
							sniffer.sniff(stmt.getStatement());
							triplesProcessed.setValue(triplesProcessed.getValue() + 1); 
							if (lstMetricConsumers != null){
								for (MetricProcess mConsumer : lstMetricConsumers) {
									try {
										mConsumer.notifyNewQuad(stmt);
							 		} catch(InterruptedException iex) {
										logger.error("[Large Stream Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
												(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, triplesProcessed.getValue(), iex.getMessage());
									}
								}
							}
						}
					}
				};
				iteratorExec.submit(iterThread);
			}
			
			iteratorExec.shutdown();
			while(!iteratorExec.isTerminated());
			System.gc();
		} catch(RiotException rex) {
			logger.error("[Large Stream Processor - {}] - Failed to process dataset: {}. RIOT Exception while attempting to process quad # : {}. Error details: {}", 
					(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, rex.getMessage());
			throw rex;
		}
		finally {
			if (lstMetricConsumers != null) {
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
			}		
		}
		
		countMetric.setCount(totalQuadsProcessed);

		
		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, this.getDatasetLocation(), sniffer.getCachingObject());
		}
		
		computeAfter();
		this.isInitalised = true;
	}
	
	
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	

		forcedCancel = true;
		for (MetricProcess mp : lstMetricConsumers){
			logger.warn("Closing and clearing quads queue for {}", mp.getMetricName());
			mp.closeAssessment();
		}
		
		logger.warn("Closing Iterators");
		try{
			this.iteratorList.forEach( (iterator) -> { iterator.close(); } );
			rdfStreamList.forEach((stream) -> { stream.finish(); } );
		}catch (RiotException re){
			logger.warn("RDF Stream already closed");
		}

		executor.shutdownNow();
	}
	
	class SharedValue<T>{
		private T value ;
		private final Object lock = new Object();
		
		SharedValue(T value) {
	        setValue(value);
	    }
	    
	    T getValue() {
	    		return value;
	    }
	    
	    void setValue(T value) {
	    		synchronized (lock) {
	    			this.value = value;
	    		}
	    }
	}
}


