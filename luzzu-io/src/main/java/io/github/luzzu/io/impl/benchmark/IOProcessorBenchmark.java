//package io.github.luzzu.io.impl.benchmark;
//
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.util.ArrayList;
//import java.util.DoubleSummaryStatistics;
//import java.util.List;
//import java.util.LongSummaryStatistics;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.jena.graph.Triple;
//import org.apache.jena.rdf.model.Model;
//import org.apache.jena.rdf.model.ModelFactory;
//import org.apache.jena.riot.RDFDataMgr;
//import org.apache.jena.riot.lang.PipedRDFIterator;
//import org.apache.jena.riot.lang.PipedRDFStream;
//import org.apache.jena.riot.lang.PipedTriplesStream;
//import org.apache.jena.vocabulary.RDF;
//import io.github.luzzu.commons.ResourceCommons;
//import io.github.luzzu.exceptions.LuzzuIOException;
//import io.github.luzzu.exceptions.ProcessorNotInitialised;
//import io.github.luzzu.io.impl.HDTProcessor;
//import io.github.luzzu.io.impl.InMemoryProcessor;
//import io.github.luzzu.io.impl.LargeNTGZStreamProcessor;
//import io.github.luzzu.io.impl.StreamProcessor;
//import io.github.luzzu.semantics.vocabularies.LMI;
//
//public class IOProcessorBenchmark {
//
//	private static long[] sizes = new long[] { 300000l , 500000l , 1000000l ,
//			5000000l , 7500000l , 10000000l };//, 25000000l , 50000000l ,
//			//100000000l , 250000000l , 500000000l , 750000000l , 1000000000l };
//	
//	private static String datasetPLD = "http://example.org";
//	private static String hdtDatasetLocation = "/Users/jeremy/Downloads/test.hdt";
//	private static String datasetLocation = "/Users/jeremy/Downloads/test.nt";
//
//	private static String warmupLocation = "/Users/jeremy/Documents/Codebase/Repository/BACKUP/quality3/examples/efo-2.34.nt";
//
//	private static Boolean generateReport = false;
//	private static Model metricConf = ModelFactory.createDefaultModel();
//	
//	public static void main (String [] args) throws InterruptedException, FileNotFoundException, LuzzuIOException {
//		metricConf.add(metricConf.createStatement(ResourceCommons.generateURI(), LMI.metric, "io.github.luzzu.testing.metrics.SimpleCountMetric"));
//
//		//warmup
////		execute(100000, 10, warmupLocation);
////		System.out.println("Warm Up Done");
////	    System.gc();
////		execute(182011590, 1, datasetLocation);
//		
//		
//		//warmup
////		RDFDataMgrBenchmark(300000l,10);
////		System.gc();
////		System.out.println("Warm Up Done");
////		for (long l : sizes) {
////			MetricProcessorBenchmark(l, 1);
////			createFiles(l);
////			RDFDataMgrBenchmark(l,10);
////		}
//		
////		RDFDataMgrBenchmark(-1,1);
//		
//		
////		System.gc();
////		System.out.println("==== In-Memory Processing Warm up ====");
////		for (int i = 0; i < 10; i++) {
////			InMemoryProcessor inm = new InMemoryProcessor(datasetPLD, datasetLocation,generateReport,metricConf);
////			inm.processorWorkFlow();
////			inm = null;
////			System.gc();
////		}
////		
////		System.out.println("==== In-Memory Processing Evaluation ====");
////		List<Long> inmTimes = new ArrayList<Long>();
////		for (int i = 0; i < 20; i++) {
////			long startParse = System.nanoTime();
////			InMemoryProcessor inm = new InMemoryProcessor(datasetPLD, datasetLocation,generateReport,metricConf);
////			inm.processorWorkFlow();
////			long endParse = System.nanoTime();
////			
////			System.out.println(String.format("[In-Memory Iteration number : %s] Time taken: %s ms",i , TimeUnit.NANOSECONDS.toMillis(endParse - startParse)));
////			inmTimes.add(TimeUnit.NANOSECONDS.toMillis(endParse - startParse));
////
////			inm = null;
////			System.gc();
////		}
//		
//		
//		System.gc();
//		System.out.println("==== Stream Processing Warm up ====");
//		for (int i = 0; i < 10; i++) {
//			StreamProcessor inm = new StreamProcessor(datasetPLD, datasetLocation,generateReport,metricConf);
//			inm.processorWorkFlow();
//			inm = null;
//			System.gc();
//		}
//		
//		System.out.println("==== Stream Processing Evaluation ====");
//		List<Long> streamTimes = new ArrayList<Long>();
//		for (int i = 0; i < 20; i++) {
//			long startParse = System.nanoTime();
//			StreamProcessor inm = new StreamProcessor(datasetPLD, datasetLocation,generateReport,metricConf);
//			inm.processorWorkFlow();
//			long endParse = System.nanoTime();
//			
//			System.out.println(String.format("[Stream Iteration number : %s] Time taken: %s ms",i , TimeUnit.NANOSECONDS.toMillis(endParse - startParse)));
//			streamTimes.add(TimeUnit.NANOSECONDS.toMillis(endParse - startParse));
//
//			inm = null;
//			System.gc();
//		}
//		
//		System.gc();
//		System.out.println("==== HDT Processing Warm up ====");
//		for (int i = 0; i < 10; i++) {
//			HDTProcessor hdt = new HDTProcessor(datasetPLD, hdtDatasetLocation,generateReport,metricConf);
//			hdt.processorWorkFlow();
//			hdt = null;
//			System.gc();
//		}
//		
//		System.out.println("==== HDT Processing Evaluation ====");
//		List<Long> hdtTimes = new ArrayList<Long>();
//		for (int i = 0; i < 20; i++) {
//			long startParse = System.nanoTime();
//			HDTProcessor hdt = new HDTProcessor(datasetPLD, hdtDatasetLocation,generateReport,metricConf);
//			hdt.processorWorkFlow();
//			long endParse = System.nanoTime();
//			
//			System.out.println(String.format("[HDT Iteration number : %s] Time taken: %s ms",i , TimeUnit.NANOSECONDS.toMillis(endParse - startParse)));
//			hdtTimes.add(TimeUnit.NANOSECONDS.toMillis(endParse - startParse));
//
//			hdt = null;
//			System.gc();
//		}
//		
//		
//		System.out.println();
//		System.out.println("+++++ Benchmark Results +++++");
//		System.out.println();
//
//		
//		DoubleSummaryStatistics hdt =  hdtTimes.stream().mapToDouble((x) -> x).summaryStatistics();
////		DoubleSummaryStatistics inMem =  inmTimes.stream().mapToDouble((x) -> x).summaryStatistics();
//		DoubleSummaryStatistics stream =  streamTimes.stream().mapToDouble((x) -> x).summaryStatistics();
//
//		
//		System.out.printf("HDT (ms) \t | \t In-Memory (ms) \t | \t  Stream (ms)\n");
//		System.out.printf("-------- \t - \t -------------- \t - \t  -----------\n");
//		System.out.printf("%s \t | \t %s \t | \t  %s\n", hdt.getAverage(), -1.0, stream.getAverage());
//	}
//	
//	private static void execute(long size, int iter, String dataset) {
//		for (int cnt = 0; cnt < iter; cnt++) {
//			LargeNTGZStreamProcessor processor = new LargeNTGZStreamProcessor(datasetPLD, dataset, generateReport, metricConf);
//			try {
//				processor.setUpProcess();
//			} catch (LuzzuIOException e1) {
//				e1.printStackTrace();
//			}
//            long start = System.nanoTime();
//			try {
//				processor.startProcessing();
//			} catch (LuzzuIOException | InterruptedException e) {
//				e.printStackTrace();
//			}
//            long write = System.nanoTime() - start;
//            System.gc();
//
//            System.out.println(String.format("[%s] %s Write took %s ms, Write/tp :%s",
//                    cnt, processor.getClass().getName(),
//                    TimeUnit.NANOSECONDS.toMillis(write), TimeUnit.SECONDS.toNanos(size) / write));
//            
//            try {
//				System.out.println(processor.getIOStats().get(0).getTriplesProcessed());
//			} catch (ProcessorNotInitialised e) {
//				e.printStackTrace();
//			}
//		}
//	}
//	
//	
////	private static void MetricProcessorBenchmark(Long size, int cnt) throws InterruptedException {
////		for (int i = 0; i <= cnt; i++) {
////			QualityMetric<Integer> met = new SimpleCountMetric();
////			MetricProcess mp = new MetricProcess(LoggerFactory.getLogger("test"), met);
////			Quad q = new Quad(null, new Triple(ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode()));
////
////            long start = System.nanoTime();
////			for (int z = 0; z < size; z++) {
////				Object2Quad newQuad = new Object2Quad(q);
////				mp.notifyNewQuad(newQuad);
////			}
////
////			while(!mp.isDoneParsing());
////            long write = System.nanoTime() - start;
////
////			System.gc();
////
////            System.out.println(String.format("[%s - %s] %s Write took %s ms, Write/tp :%s",
////                    cnt,size, mp.getClass().getName(),
////                    TimeUnit.NANOSECONDS.toMillis(write), TimeUnit.SECONDS.toNanos(size) / write));
////		}
////	}
//	
//	private static void createFiles(long size) throws FileNotFoundException {
//		Model m = ModelFactory.createDefaultModel();
//		for (int i = 0; i <= size; i++) {
//			m.add(ResourceCommons.generateURI(), RDF.type, ResourceCommons.generateURI());
//		}
//		m.write(new FileOutputStream(new File("/Users/jeremy/Documents/Codebase/Repository/Luzzu/luzzu-io/src/test/resources/Benchmark/"+size+".nt")), "N-TRIPLE");
//	}
//	
//	
//	private static void RDFDataMgrBenchmark(long size, int iter) throws FileNotFoundException {
//		for(int i = 0; i < iter; i++) {
//			int buffer = 0;
//			if (size == -1) buffer = (int) (182011590 * 0.10);
//			else buffer = (int) (size * 0.10);
////			int buffer = (int) (182011590 * 0.10);
//			PipedRDFIterator<Triple> iterator =  new PipedRDFIterator<Triple>(buffer, true, 50 , 10000);
//			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
//			ExecutorService executor = Executors.newSingleThreadExecutor();
//			
//			String loc = "";
//			if (size == -1) loc = datasetLocation;
//			else loc = "/Users/jeremy/Documents/Codebase/Repository/Luzzu/luzzu-io/src/test/resources/Benchmark/"+size+".nt";
//			
//			final String dsL = loc;
//			long start = System.nanoTime();
//			Runnable parser = new Runnable() {
//				public void run() {
//					try{
//						long startParse = System.nanoTime();
//						RDFDataMgr.parse(rdfStream, dsL);
//						long endParse = System.nanoTime() - startParse;
//						System.out.println(String.format("Parsing Time: %s", TimeUnit.NANOSECONDS.toMillis(endParse)));
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			};
//			executor.submit(parser);
//			long totalQuadsProcessed = 0;
//			
//			try {
//				while (iterator.hasNext()) {
//					iterator.next();
//					totalQuadsProcessed++;
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			long write = System.nanoTime() - start;
//			
//			iterator.close();
//			executor.shutdown();
//			executor = null;
//			System.gc();
//			
//			if (size == -1) {
//				System.out.println(String.format("[%s - %s] Write took %s ms, Write/tp :%s. Quads Processed: %s",  i ,size,
//		                  TimeUnit.NANOSECONDS.toMillis(write), TimeUnit.SECONDS.toNanos(182011590) / write, totalQuadsProcessed));
//
//			} else {
//				System.out.println(String.format("[%s - %s] Write took %s ms, Write/tp :%s. Quads Processed: %s",  i ,size,
//		                  TimeUnit.NANOSECONDS.toMillis(write), TimeUnit.SECONDS.toNanos(size) / write, totalQuadsProcessed));
//			}
//
//		}
//	}
//}
