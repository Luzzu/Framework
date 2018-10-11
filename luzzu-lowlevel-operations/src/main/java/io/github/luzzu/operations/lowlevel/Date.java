package io.github.luzzu.operations.lowlevel;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Date {
	final static Logger logger = LoggerFactory.getLogger(Date.class);
	
	protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat rdfFormat = new SimpleDateFormat("yyyy-MM-dd");

	protected java.util.Date javaDate = new java.util.Date();

	public Date() { }
	
	public String getDate() {
		return sdf.format(javaDate.getTime());
	}
	
	public String getRDFFormatDate() {
		return rdfFormat.format(javaDate.getTime());
	}
	
	public static String getRDFFormatDate(String date) throws ParseException {
		SimpleDateFormat fm = new SimpleDateFormat("yyyy-MM-dd");
		return fm.format(fm.parse(date));		
	}
	
	public static String dateToString(java.util.Date date) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static java.util.Date stringToDate(String date) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.parse(date);
	}
}