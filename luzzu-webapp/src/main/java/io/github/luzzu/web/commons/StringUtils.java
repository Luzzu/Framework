package io.github.luzzu.web.commons;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class StringUtils {

	/**
	 * Given a dataset URI, the function returns a stripped version 
	 * that can be used within Luzzu's Internal Structure to find
	 * the exact graph.
	 * 
	 * @param (String) Dataset URI
	 * @return (String) Modified dataset graph URI
	 */
	public static String strippedURI(String datasetURI){
		String stripped = datasetURI.replace("http://", "");
		if (stripped.charAt(stripped.length() - 1) == '/'){
			stripped = stripped.substring(0,stripped.length() - 1);
		}
		return stripped;
	}
	
	
	/**
	 * Loads Luzzu preset query into a String Variable.
	 * Throws exception if file does not exists.
	 * 
	 * @param (String) Filename or Location of SPARQL query
	 * @return (String) SPARQL query
	 * @throws IOException
	 */
	public static String getQueryFromFile(String fileName) throws IOException {
		URL url = Resources.getResource(fileName);
		return Resources.toString(url, Charsets.UTF_8);
	}
	
	
	public static String toJSONDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try{
			return sdf.format(sdf.parse(date));
		} catch (ParseException e){
			sdf = new SimpleDateFormat("yyyy-MM-dd");
			return sdf.format(sdf.parse(date));
		}
	}
}
