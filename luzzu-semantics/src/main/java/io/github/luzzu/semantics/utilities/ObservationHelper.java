package io.github.luzzu.semantics.utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.semantics.datatypes.Observation;
import io.github.luzzu.semantics.vocabularies.CUBE;
import io.github.luzzu.semantics.vocabularies.DAQ;

public class ObservationHelper {
	public static List<Observation> extractObservations(Model qualityMD, Resource metric){
		List<Observation> lst = new ArrayList<Observation>(); 
		
		ResIterator itRes = qualityMD.listResourcesWithProperty(RDF.type, metric);
		if (!(itRes.hasNext())) return lst;
		Resource resNode = itRes.next();
		NodeIterator iter = qualityMD.listObjectsOfProperty(resNode, DAQ.hasObservation);
		while(iter.hasNext()){
			Resource res = iter.next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, qualityMD.createProperty("http://purl.org/linked-data/sdmx/2009/dimension#timePeriod")).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			//get value
			Object value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getValue();
			
			//get computedOn
			Resource computedOn = qualityMD.listObjectsOfProperty(res,DAQ.computedOn).next().asResource();
			
			//data cube
			Resource cubeDS = qualityMD.listObjectsOfProperty(res, CUBE.dataSet).next().asResource();
			
			//is estimate
			Boolean isEstimate = qualityMD.listObjectsOfProperty(res, DAQ.isEstimate).next().asLiteral().getBoolean();
			
			Observation obs = new Observation(res, date, value, value.getClass().getSimpleName(), computedOn, cubeDS, isEstimate);
			lst.add(obs);
		}
		
		return lst;
	}
	
	private static Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		try{
			return sdf.parse(date);
		} catch (ParseException e){
			sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
			return sdf.parse(date);
		}
	}
	
	public static Observation getLatestObservation(List<Observation> observations){
		Collections.sort(observations);
		Collections.reverse(observations);
		return observations.get(0);
	}
	
	public static Map<Resource,List<Observation>> extractAllObservations(Model qualityMD){
		Map<Resource,List<Observation>> map = new HashMap<Resource,List<Observation>>(); 
		
		NodeIterator iter = qualityMD.listObjectsOfProperty(null, DAQ.hasObservation);
		while(iter.hasNext()){
			Resource res = iter.next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, qualityMD.createProperty("http://purl.org/linked-data/sdmx/2009/dimension#timePeriod")).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			//get value
			Object value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getValue();
			
			//get computedOn
			Resource computedOn = qualityMD.listObjectsOfProperty(res,DAQ.computedOn).next().asResource();
			
			//data cube
			Resource cubeDS = qualityMD.listObjectsOfProperty(res, CUBE.dataSet).next().asResource();
			
			//get metric
			Resource metricURI = qualityMD.listObjectsOfProperty(res, DAQ.metric).next().asResource();
			Resource metric = qualityMD.listObjectsOfProperty(metricURI, RDF.type).next().asResource();			
			
			//is estimate
			Boolean isEstimate = qualityMD.listObjectsOfProperty(res, DAQ.isEstimate).next().asLiteral().getBoolean();
			
			Observation obs = new Observation(res, date, value, value.getClass().getSimpleName(), computedOn, cubeDS, isEstimate);
			List<Observation> lst = new ArrayList<Observation>(); 
			lst.add(obs);
			map.merge(metric, lst, (vOld, vNew) -> {
				vNew.addAll(vOld);
				return vNew;
			});
		}
		
		return map;
	}
}