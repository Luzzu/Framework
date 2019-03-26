package io.github.luzzu.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import io.github.luzzu.operations.properties.PropertyManager;

public class LoadToDataStore
{
	protected String host;
	protected String port;
	protected String dataStore;
	protected String fusekiUserName;
	protected String fusekiPassword;

	final static Logger logger = LoggerFactory.getLogger(LoadToDataStore.class);

	public LoadToDataStore()
	{

		// Load properties from configuration files
		PropertyManager props = PropertyManager.getInstance();
		host = props.getProperties("luzzu.properties").getProperty("FUSEKI_SERVER");
		port = props.getProperties("luzzu.properties").getProperty("FUSEKI_PORT");
		dataStore = props.getProperties("luzzu.properties").getProperty("FUSEKI_DATASTORE");
		fusekiUserName = props.getProperties("luzzu.properties").getProperty("FUSEKI_USERNAME");
		fusekiPassword = props.getProperties("luzzu.properties").getProperty("FUSEKI_PASSWORD");

	}

	public void loadData(String graphName, String filePath) 
	{

		logger.info("Load to Datasote initiated - Host : "+host+", Port : "+port + ", Datastore : "+ dataStore);

		String protocol = "http";
		String connectionURL = host+":"+port+"/"+dataStore+"/data";
		connectionURL= connectionURL.replace("//", "/");
		connectionURL= connectionURL.replace("::", ":");
		connectionURL=protocol+"://"+connectionURL;
		String connectionGetURL=host+":"+port+"/$/stats/" + dataStore;
		connectionGetURL= connectionGetURL.replace("//", "/");
		connectionGetURL= connectionGetURL.replace("::", ":");
		connectionGetURL=protocol+"://"+connectionGetURL;
			
		File uploadTripleFile = new File(filePath);
		//Build Base URI
		URIBuilder uriBuilder = null;
		try {
			uriBuilder = new URIBuilder(connectionURL);
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}

		//Add Parameters to URI. I.e Graph Name
		//Only add graph name for Problem Reports
		int lastIndexOfDot = filePath.lastIndexOf('.');
		String fileExtension = filePath.substring(lastIndexOfDot+1).toUpperCase();
		if (fileExtension.equalsIgnoreCase("TTL"))
		{
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			params.add(new BasicNameValuePair("graph", graphName));
			uriBuilder.addParameters(params);
		}


		//Authenticaion
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		Credentials credentials = new UsernamePasswordCredentials(fusekiUserName, fusekiPassword);
		credsProvider.setCredentials(AuthScope.ANY, credentials);

		//Build HTTP Client with Authentication
		CloseableHttpClient httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.build();

		//HTTP Post Request Object
		HttpPost httpPost = null;
		try {
			httpPost = new HttpPost(uriBuilder.build());
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		//HTTP Post Request Headers
		httpPost.addHeader("Accept", "application/json");
		//httpPost.addHeader("Content-Type", "multipart/form-data");




		//Set Body with the File
		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.addBinaryBody("files", uploadTripleFile);
		HttpEntity multipart = builder.build();
		httpPost.setEntity(multipart);


		try {
			try{
			CloseableHttpResponse responseGet = httpclient.execute(new HttpGet(connectionGetURL));
			}
			catch(ClientProtocolException e)
			{
				logger.warn("Client Protocol Exception during Datastore Status Check");
				e.printStackTrace();	
			}
			CloseableHttpResponse response = httpclient.execute(httpPost);
			httpclient.close();
		} catch (ClientProtocolException e) {
			System.out.println("Client Protocol Exception");
			logger.error("Client Protocol Exception during Load to Datasote");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception");
			e.printStackTrace();
		}


		logger.info("Load to Datasote Completed");


	}


}
