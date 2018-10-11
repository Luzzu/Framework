package io.github.luzzu.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotEnabled;
import io.github.luzzu.io.impl.HDTProcessor;
import io.github.luzzu.io.impl.InMemoryProcessor;
import io.github.luzzu.io.impl.LargeNTGZStreamProcessor;
import io.github.luzzu.io.impl.SPARQLEndPointProcessor;
import io.github.luzzu.io.impl.StreamProcessor;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;

public class ProcessorController {

	final static Logger logger = LoggerFactory.getLogger(ProcessorController.class);

	private Properties processorProperties = PropertyManager.getInstance().getProperties("luzzu.properties");
	
	private static ProcessorController instance = null;
	
	private ProcessorController(){};
	
	public static ProcessorController getInstance(){
		if (instance == null) instance = new ProcessorController();
		return instance;
	}
	
	public IOProcessor decide(String baseURI, String datasetURI, boolean genQualityReport, Model modelConfig, boolean isSparql, String crawlDate) throws LuzzuIOException {
		if (isSparql) {
			if (crawlDate == null)
				return new SPARQLEndPointProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
			else 
				return new SPARQLEndPointProcessor(baseURI, datasetURI, genQualityReport, modelConfig,crawlDate);
		} else if (datasetURI.endsWith("hdt")) {
			System.out.println("Using an HDT Processor for the Assessment");
			logger.info("[Processor Controller] Choice: HDT Processor");
			
			if (Boolean.parseBoolean(processorProperties.getProperty("USE_HDT_PROCESSOR"))) {
				if (crawlDate == null)
					return new HDTProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
				else 
					return new HDTProcessor(baseURI, datasetURI, genQualityReport, modelConfig, crawlDate);
			} else {
				throw new ProcessorNotEnabled("HDT Processor is not enabled. Please enable from the luzzu properties settings: USE_HDT_PROCESSOR=true");
			}
		} else {
			long freeMemory = Runtime.getRuntime().freeMemory();
			logger.warn("[Processor Controller] free memory: "+(double)freeMemory / (1024.0*1024.0) + " MB");
			File file =new File(datasetURI);
				
			long length = file.length();
			
			boolean aGZFile = false;
			if (datasetURI.endsWith("gz")){
				aGZFile = true;
				RandomAccessFile raf;
				try {
					raf = new RandomAccessFile(file, "r");
					raf.seek(raf.length() - 4);
					int b4 = raf.read();
					int b3 = raf.read();
					int b2 = raf.read();
					int b1 = raf.read();
					length = Math.abs((b1 << 24) | (b2 << 16) + (b3 << 8) + b4);
					raf.close();
				} catch (IOException e) {
					ExceptionOutput.output(e, "[Processor Controller] Error reading "+datasetURI, logger);
				}
			}
			
			logger.warn("[Processor Controller] file size: " + (double) length / (1024.0*1024.0) + " MB");
			
			if (length <= ((double) freeMemory * (1.0 / 10.0))) {
				// if it fits in 1/100 of the free memory, then we use a memory processor
				if (Boolean.parseBoolean(processorProperties.getProperty("USE_INMEMORY_PROCESSOR"))) {
					System.out.println("Using an In-Memory Processor for the Assessment");
					logger.info("[Processor Controller] Choice: In-Memory");
					if (crawlDate == null)
						return new InMemoryProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
					else 
						return new InMemoryProcessor(baseURI,datasetURI,genQualityReport,modelConfig,crawlDate);
				} else {
					System.out.println("Using a Stream Processor for the Assessment");
					logger.info("[Processor Controller] Choice: Stream");
					if (crawlDate == null)
						return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
					else
						return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig,crawlDate);
				}
			} else if (length <= ((double) freeMemory * (1.0 / 2.0))) {
				System.out.println("Using a Stream Processor for the Assessment");
				logger.info("[Processor Controller] Choice: Stream");
				if (crawlDate == null)
					return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
				else
					return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig,crawlDate);
			}
			else {
				if ((aGZFile) && (Boolean.parseBoolean(processorProperties.getProperty("USE_NTGZSTREAM_PROCESSOR")))) {
					System.out.println("Using a Buffered Stream Processor for the Assessment");
					logger.info("[Processor Controller] Choice: Large");
					if (crawlDate == null)
						return new LargeNTGZStreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig); //e.g for dbpedia etc
					else 
						return new LargeNTGZStreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig,crawlDate);
				} else {
					if (!aGZFile) 
						System.out.println("Using a Stream Processor for the Assessment. File is Large, consider compression using GunZip");
					if (!(Boolean.parseBoolean(processorProperties.getProperty("USE_NTGZSTREAM_PROCESSOR")))) 
						System.out.println("Using a Stream Processor for the Assessment. File is Large, consider enabling the Large NTGZ processor in the properties file: USE_NTGZSTREAM_PROCESSOR=true ");
					logger.info("[Processor Controller] Choice: Stream");
					if (crawlDate == null)
						return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
					else
						return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig,crawlDate);
				}
			}	
		}
	}
}