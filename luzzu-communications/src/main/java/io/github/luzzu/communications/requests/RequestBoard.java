package io.github.luzzu.communications.requests;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.jena.query.Dataset;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.github.luzzu.communications.ExtendedCallable;
import io.github.luzzu.communications.exceptions.ResourceNotFoundException;
import io.github.luzzu.communications.utils.CollectionToJSON;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.io.helper.IOStats;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.web.dataset.Profiling;
import io.github.luzzu.web.dataset.Quality;
import io.github.luzzu.web.export.MetadataExport;
import io.github.luzzu.web.jsondatatypes.ComparingDatasetObject;
import io.github.luzzu.web.jsondatatypes.ComputedMetric;


/**
 * The RequestBoard Class is a "static" class where
 * all requests to Luzzu are stored. More specifically,
 * this class acts as a router to functions such as 
 * getting the statistics of a particular request,
 * and cancel an assessment, amongst others.
 * 
 * @author Jeremy Debattista
 * @version 4.0.0
 *
 */
public class RequestBoard {

	final static Logger logger = LoggerFactory.getLogger(RequestBoard.class);
	
	private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("requestboard-addrequest-thread-%d").build();
	private static ExecutorService executor = Executors.newFixedThreadPool(12, namedThreadFactory); //TODO: update from settings?
	private static ListeningExecutorService service = MoreExecutors.listeningDecorator(executor);

	private static Map<AssessmentRequest,Future<Boolean>> computingResources = new ConcurrentHashMap<AssessmentRequest, Future<Boolean>>(); // resources under assessment
	private static Map<AssessmentRequest, Callable<Boolean>> callableDirectory = new ConcurrentHashMap<AssessmentRequest, Callable<Boolean>>(); // holds all requests instances 
	private static Map<String, AssessmentRequest> requestObjects = new ConcurrentHashMap<String, AssessmentRequest>();
			
		
	private static Set<String> finishedResources = new HashSet<String>(); // completed assessment requests
	
	static {
		ThreadFactory namedBackgroundThread = new ThreadFactoryBuilder().setNameFormat("requestboard-background-thread-%d").build();
		ScheduledExecutorService execService =   Executors.newScheduledThreadPool(1, namedBackgroundThread);
		execService.scheduleAtFixedRate(()->{
			computingResources.forEach((req, future) -> {
				try {
					getRequestStatus(req.getRequestID());
				} catch (InterruptedException | ExecutionException | ResourceNotFoundException e) {
					ExceptionOutput.output(e, "Request Scheduler Exception", logger);
					computingResources.remove(req);
				}
			});
		}, 0, 2, TimeUnit.SECONDS);
	}
	
	public static String addRequest(Callable<Boolean> request, String dataset_location, String pld, boolean isSparql){
	    	ListenableFuture<Boolean> handler = service.submit(request);

	    	AssessmentRequest reqObj = new AssessmentRequest(dataset_location, pld, isSparql);
	    	computingResources.put(reqObj, handler);
	    	callableDirectory.put(reqObj, request);
	    	requestObjects.put(reqObj.getRequestID(), reqObj);
	    	
	    	Futures.addCallback(handler, new FutureCallback<Boolean>() {
				@Override
				public void onFailure(Throwable e) {
					try {
						ExceptionOutput.output(new Exception(e), "Assessment Request Failed Exception", logger);
						RequestBoard.cancelRequest(reqObj.getRequestID());
					} catch (LuzzuIOException ioe) {
						reqObj.setStatus(AssessmentStatus.FAILED);
					} catch (ResourceNotFoundException e1) {
						ExceptionOutput.output(e1, "Assessment Request Future Callback Failure Exception", logger);
					}
				}

				@Override
				public void onSuccess(Boolean arg0) {
					try {
						RequestBoard.getRequestStatus(reqObj.getRequestID());
					} catch (InterruptedException | ExecutionException | ResourceNotFoundException e) {
						ExceptionOutput.output(e, "Assessment Request Future Callback Success Exception", logger);
					}
				}
	    	});
	    		    	
	    	return reqObj.getRequestID();
	}
	
    public static String getRequestStatus(String uuid) throws InterruptedException, ExecutionException, ResourceNotFoundException{
    		AssessmentRequest req = requestObjects.get(uuid);
    		if (req != null) {
	    		if (!finishedResources.contains(uuid)) {
	        		Future<Boolean> handler = computingResources.get(req);
	        		if (handler.isDone()){
	        			Boolean result = handler.get();
	        			computingResources.remove(req);
	        			requestObjects.remove(uuid);
	        			callableDirectory.remove(req);
	        					
	        			if (result) {
	        				req.setStatus(AssessmentStatus.SUCCESSFUL);
	        			} else {
	        				req.setStatus(AssessmentStatus.FAILED);
	        			}
	        			finishedResources.add(uuid);

	        			requestObjects.put(uuid,req);
	        		} 
	    		}
	    		return req.toString();
	    	} else {
	    		throw new ResourceNotFoundException(uuid);
	    	}
    }
    
    public static String getAllRequests(AssessmentStatus status){
    		List<AssessmentRequest> pendingRequests = new ArrayList<AssessmentRequest>();
    		
    		for (AssessmentRequest ar : requestObjects.values()) {
    			if (status == AssessmentStatus.FAILED) {
    				if ((ar.getStatus() == status) || (ar.getStatus() == AssessmentStatus.CANCELLED)) pendingRequests.add(ar);
    			} else {
    				if (ar.getStatus() == status) pendingRequests.add(ar);
    			}
    		}
    	
	    	return CollectionToJSON.convert(pendingRequests, "Results");
    }
    
    public static String cancelRequest(String uuid) throws LuzzuIOException, ResourceNotFoundException{
		AssessmentRequest req = requestObjects.get(uuid);
		if (req != null) {
	    		if (!finishedResources.contains(uuid)) {
	        		Future<Boolean> handler = computingResources.get(req);
	        		
		    		ExtendedCallable<Boolean> callable = (ExtendedCallable<Boolean>) callableDirectory.get(req);
		    		callable.getIOProcessor().cancelMetricAssessment();
		    		handler.cancel(true);
		    		
		    		req.setStatus(AssessmentStatus.CANCELLED);
		    		finishedResources.add(uuid);
		    		requestObjects.put(uuid,req);
	    		}
	    		return req.toString();
		} else {
			throw new ResourceNotFoundException(uuid);
		}
    }
    
    public static String getRequestStatistics(String uuid) throws LuzzuIOException, ResourceNotFoundException {
    		AssessmentRequest req = requestObjects.get(uuid);
		if (req != null) {
	    		if (!finishedResources.contains(uuid)) {
	    			ExtendedCallable<Boolean> callable = (ExtendedCallable<Boolean>) callableDirectory.get(req);
					List<IOStats> stats = callable.getIOProcessor().getIOStats();
					AssessmentStatistics as = new AssessmentStatistics(uuid, stats);
					return as.toString();
	    		} else {
	    			return req.toString(); 
	    		}
		} else {
			throw new ResourceNotFoundException(uuid);
		}
    }
    
    public static String getProfilingForObservation(String observationURI, String datasetPLD) {
    		return Profiling.getProfilingInformation(observationURI, datasetPLD).toString();
    }
    
    public static String getLatestValuesForDataset(String datasetPLD) {
    		Set<ComputedMetric> cm = Quality.getLatestObservationForDataset(datasetPLD);
    		return CollectionToJSON.convert(cm, "Metrics");
    }
    
    public static String getAssessmentDatesForDataset(String datasetPLD) {
		Set<String> cm = Quality.getObservationDates(datasetPLD);
		return CollectionToJSON.convert(cm, "Assessment-Dates");
    }
    
    public static String getValuesForDatasetWithDate(String datasetPLD, String date) {
		Set<ComputedMetric> cm = Quality.getObservationForDataset(datasetPLD, date);
		return CollectionToJSON.convert(cm, "Metrics");
    }
    
    public static String compareDatasetsOnMetric(Set<String> datasetPLDs, String metric) {
		Set<ComparingDatasetObject> objects = Quality.compareDatasetsOnMetric(datasetPLDs, metric);
		return CollectionToJSON.convert(objects, "Datasets");
    }
    
    public static String getMetadataInDQV(String datasetPLD) {
    		Dataset d = MetadataExport.exportToDQV(datasetPLD);
    		
    		StringWriter writer = new StringWriter();
    		RDFDataMgr.write(writer, d, RDFFormat.TRIG_PRETTY);
    		
    		return writer.toString();
    }
    
    public static String getMetadataInDAQ(String datasetPLD) {
		Dataset d = MetadataExport.exportToDAQ(datasetPLD);
		
		StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, d, RDFFormat.TRIG_PRETTY);
		
		return writer.toString();
}
}
