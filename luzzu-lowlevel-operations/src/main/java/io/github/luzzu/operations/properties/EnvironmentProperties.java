package io.github.luzzu.operations.properties;

import org.apache.commons.lang3.NotImplementedException;

/**
 * @author Jeremy Debattista
 *
 * This class makes visible all variable properties in
 * the framework
 */
public class EnvironmentProperties {

	protected static String datasetURI = "";
	private static EnvironmentProperties instance = null;
	
	protected EnvironmentProperties(){	}
	
	public static EnvironmentProperties getInstance(){
		if (instance == null) instance = new EnvironmentProperties();
		return instance;
	}

	/**
	 * Returns the location (file or url) of the dataset being assessed.
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public String getDatasetLocation() throws IllegalStateException {
		if (PropertyManager.getInstance().environmentVars.containsKey("dataset-location")){
			return PropertyManager.getInstance().environmentVars.get("dataset-location");
		} else {
			throw new IllegalStateException("Dataset is not Set");
		}
	}
	
	
	/**
	 * Returns the dataset's PLD being assessed.
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public String getDatasetPLD() throws IllegalStateException {
		if (PropertyManager.getInstance().environmentVars.containsKey("dataset-pld")){
			return PropertyManager.getInstance().environmentVars.get("dataset-pld");
		} else {
			throw new IllegalStateException("Processor is not initialised yet");
		}
	}
	
	/**
	 * Returns true if the assessor requires a problem report to be generated.
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public Boolean requiresQualityProblemReport() throws IllegalStateException {
		if (PropertyManager.getInstance().environmentVars.containsKey("require-quality-report")){
			return Boolean.valueOf(PropertyManager.getInstance().environmentVars.get("require-quality-report"));
		} else {
			throw new IllegalStateException("Processor is not initialised yet");
		}
	}
	
	/**
	 * Returns the number of data lines (or triples) of the dataset being assessed
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public Long getTotalNumberDataLines() throws IllegalStateException {
		throw new NotImplementedException("The function getTotalNumberDataLines() is not implemented yet");
	}
	
	/**
	 * Returns an estimate number of data lines (or triples) of the dataset being assessed
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public Long getEstimateNumberDataLines() throws IllegalStateException {
		throw new NotImplementedException("The function getEstimateNumberDataLines() is not implemented yet");
	}
}
