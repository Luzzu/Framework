package io.github.luzzu.qualitymetrics.commons.cache;

import io.github.luzzu.operations.cache.CacheObject;

/**
 * @author Jeremy Debattista
 * 
 * This cache handles external vocabularies downloaded
 * for various metrics (such as UndefinedClassesAndProperties) 
 */
public class CachedVocabulary implements CacheObject {

	private static final long serialVersionUID = -4829960786875890863L;

	private String ns = "";
	private String textualContent = "";
	private String language = "";

	
	public String getNs() {
		return ns;
	}
	
	public void setNs(String ns) {
		this.ns = ns;
	}
	
	public String getTextualContent() {
		return textualContent;
	}
	
	public void setTextualContent(String textualContent) {
		this.textualContent = textualContent;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
}