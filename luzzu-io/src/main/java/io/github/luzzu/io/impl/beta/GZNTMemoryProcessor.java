package io.github.luzzu.io.impl.beta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;

public class GZNTMemoryProcessor extends AbstractIOProcessor {

	final static Logger logger = LoggerFactory.getLogger(GZNTMemoryProcessor.class);
	
	private boolean isInitalised = false;
	
	// RDFIterator parameters
	private final int rdfIterBufferSize = PipedRDFIterator.DEFAULT_BUFFER_SIZE * 2;
	private final int rdfIterPollTimeout = 10000;
	private final int rdfIterMaxPolls = 50;
	private final boolean rdfIterFairBufferLock = true;
	protected RandomAccessFile raf;
	protected PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	
	public GZNTMemoryProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration){
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = GZNTMemoryProcessor.logger;
	}
	
	public GZNTMemoryProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = GZNTMemoryProcessor.logger;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void setUpProcess() {

		Lang lang  = RDFLanguages.filenameToLang(this.datasetLocation);
		
		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			this.iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			this.iterator = new PipedRDFIterator<Triple>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
		}
		
		this.isInitalised = true;
		
		try {
			this.loadMetrics();
		} catch (ExternalMetricLoaderException e) {
			logger.error(e.getLocalizedMessage());
		}
		
		this.executor = Executors.newSingleThreadExecutor();
	}

	@Override
	public void startProcessing() throws LuzzuIOException, InterruptedException {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		long totalQuadsProcessed = 0;
				
		try {
			
			final InputStream in = Channels.newInputStream(raf.getChannel());
			
			final String dsLoc = this.datasetLocation;
			
			Runnable parser = new Runnable() {
				public void run() {
					try{
						RDFDataMgr.parse(rdfStream, in, Lang.NTRIPLES);
					} catch (Exception e) {
						logger.error("Error parsing dataset {}. Error message {}", dsLoc, e.getMessage());
					}
				}
			};
			
			executor.submit(parser);

			while (this.iterator.hasNext()) {
				totalQuadsProcessed++;				
				Object2Quad stmt = new Object2Quad(this.iterator.next());
				sniffer.sniff(stmt.getStatement());
				
				if (lstMetricConsumers != null){
					for (MetricProcess mConsumer : lstMetricConsumers) {
						try {
							mConsumer.notifyNewQuad(stmt);
						} catch(InterruptedException iex) {
							logger.warn("[GZNT Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
									(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, iex.getMessage());
							throw iex;
						}
					}
				}
			}
		} catch(RiotException rex) {
			logger.warn("[Stream Processor - {}] - Failed to process dataset: {}. RIOT Exception while attempting to process quad # : {}. Error details: {}", 
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
		
		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, this.datasetLocation, sniffer.getCachingObject());
		}

		computeAfter();
	}

	@Override
	public void cleanUp() throws ProcessorNotInitialised {
		this.isInitalised = false;
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
		
		File f = new File ("/tmp/"+tempFileID);
		f.delete();
	}

	@Override
	public void processorWorkFlow() throws LuzzuIOException, InterruptedException{
		this.setUpProcess();
		
		try {
			loadGZNTfileInRAM();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
			
		try {
			this.startProcessing();
		} catch (ProcessorNotInitialised e) {
			logger.warn("Processor not initialized while trying to start dataset processing. Dataset: {}", this.datasetLocation);
			this.processorWorkFlow();
		}
		
		if (!forcedCancel){
			this.generateAndWriteQualityMetadataReport();
			if (this.genQualityReport) {
				this.generateAndWriteQualityProblemReport();
			}
		}  	
	}
	
	private String tempFileID = "";
	private void loadGZNTfileInRAM() throws IOException{
		FileInputStream fileIn = new FileInputStream(this.datasetLocation);
		GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
		this.tempFileID = UUID.randomUUID().toString();
		FileOutputStream fileOutputStream = new FileOutputStream("/tmp/"+tempFileID);

		int bytes_read;
		byte[] buffer = new byte[1024];
		while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
			fileOutputStream.write(buffer, 0, bytes_read);
		}

		gZIPInputStream.close();
		fileOutputStream.close();


		this.raf = new RandomAccessFile("/tmp/"+tempFileID,"r");
	}
	
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	

		for (MetricProcess mp : lstMetricConsumers){
			logger.info("Closing and clearing quads queue for {}", mp.getMetricName());
			mp.closeAssessment();
		}
		
		logger.info("Closing Iterators");
		this.iterator.close();
		this.rdfStream.finish();

		executor.shutdownNow();
	}
}
