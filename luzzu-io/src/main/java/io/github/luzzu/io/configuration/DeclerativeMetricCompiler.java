package io.github.luzzu.io.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.qml.datatypes.conditions.CustomCondition;
import io.github.luzzu.qml.parser.LQMLParser;
import io.github.luzzu.qml.parser.ParseException;


public class DeclerativeMetricCompiler {
	
	final static Logger logger = LoggerFactory.getLogger(DeclerativeMetricCompiler.class);

	private static DeclerativeMetricCompiler instance = null;

	private final StringBuilder javaClass = new StringBuilder();
	private Map<String,CustomCondition> customConditions = new HashMap<String,CustomCondition>();
	
	public DeclerativeMetricCompiler() throws URISyntaxException, IOException{
		this.loadCustomFunctions();
		this.loadDeclerativePattern(); 
	}

	public static DeclerativeMetricCompiler getInstance(){
		if (instance  == null) {
			logger.info("[DeclerativeMetricCompiler] Initialising and verifying external metrics.");
			
			try {
				
				if (Files.notExists(Paths.get("classes/"), LinkOption.NOFOLLOW_LINKS)) {
					Files.createDirectory(Paths.get("classes/"));
				}
				
				instance = new DeclerativeMetricCompiler();
				
			} catch (URISyntaxException | IOException e) {
				ExceptionOutput.output(e, "[DeclerativeMetricCompiler] - Error creating singleton instance", logger);
			} 
		}
		
		return instance;
	}
	
	private static IOFileFilter javaFilter = new IOFileFilter() {
		
		@Override
		public boolean accept(File arg0, String arg1) {
			if(arg1.endsWith(".java")) {
				return true;
			}
			return false;
		}
		
		@Override
		public boolean accept(File file) {
			if (file.getName().endsWith(".java")) {
				return true;
			}
			return false;
		}
	};
	
	private static IOFileFilter lqmFilter = new IOFileFilter() {
		
		@Override
		public boolean accept(File arg0, String arg1) {
			if(arg1.endsWith(".lqm")) {
				return true;
			}
			return false;
		}
		
		@Override
		public boolean accept(File file) {
			if (file.getName().endsWith(".lqm")) {
				return true;
			}
			return false;
		}
	};
	@SuppressWarnings({ "unchecked", "resource" })
	public Map<String, Class<? extends QualityMetric<?>>> compile() throws IOException, ParseException {
		Map<String, Class<? extends QualityMetric<?>>> clazzes = new HashMap<String, Class<? extends QualityMetric<?>>>();
		
		//get system compiler:
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        
        // for compilation diagnostic message processing on compilation WARNING/ERROR
        MyDiagnosticListener c = new MyDiagnosticListener();
		
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(c, Locale.ENGLISH,null);
        
        File f = new File("classes/");
        if (!f.exists()) f.mkdir();
        
        //specify classes output folder
        String currentDir = new File(".").getAbsolutePath();
        String classpath = currentDir + File.separator + "classes"+ File.separator
          + System.getProperty("path.separator")
          + System.getProperty("java.class.path") + System.getProperty("path.separator")
          + System.getProperty("surefire.test.class.path");
        List<String> options = Arrays.asList("-d", "classes/","-source","1.8", "-target", "1.8","-classpath",classpath);
	    
		// parse and compile declerative functions
		Set<URI> lqiSet = this.loadMetrics();
		
		if (lqiSet.size() > 0){
		try {
			this.loadDeclerativePattern();
		} catch (URISyntaxException e1) {
			ExceptionOutput.output(e1, "[Declerative Metric Loader] - Error in loading Declerative Pattern", logger);
		}}
		
		
		for(URI lqiMetric : lqiSet){
			//parse
			Tuple parsed = this.parse(lqiMetric);
			
			JavaFileObject so = null;
			try
			{
				so = new InMemoryJavaFileObject(parsed.className, parsed.javaClass);
			} catch (Exception exception){
				exception.printStackTrace();
			}
			
			Iterable<? extends JavaFileObject> files = Arrays.asList(so);
		    
			//compile
			JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager,c, options, null, files);
	        
			task.call();

			//create classes
			File file = new File("classes/");
			URL uri = file.toURI().toURL();
			URL[] urls = new URL[] { uri };
			
			ClassLoader loader = new URLClassLoader(urls);
			
			Class<? extends QualityMetric<?>> clazz = null;
			try{
				clazz = (Class<? extends QualityMetric<?>>) loader.loadClass(parsed.className);
			} catch (ClassNotFoundException e) {
				logger.error("Class {} is not found. Skipped loading the class.", parsed.className );
				continue;
			}

			clazzes.put(parsed.className, clazz);
		}
		
	    fileManager.close();
	    
	    return clazzes;
	}
	
	
	private void loadDeclerativePattern() throws URISyntaxException, IOException{
		
		String nextLine = null;
		BufferedReader reader = null;
		this.javaClass.setLength(0);
		try {
			reader = new BufferedReader(
					new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("declerative_pattern.txt"), 
					Charset.defaultCharset()));
			
			while((nextLine = reader.readLine()) != null) {
				this.javaClass.append(nextLine);
			}
		} finally {
			if(reader != null) {
				reader.close();
			}
		}
	}
	
    private static final Pattern PACKAGE_PATTERN = Pattern.compile("^package\\s+(\\w+(\\.\\w+)*);", Pattern.MULTILINE);
    private static final Pattern CLASSNAME_PATTERN = Pattern.compile("^public\\s+(?:final\\s+)?class\\s+(\\w+)",Pattern.MULTILINE);

	private void loadCustomFunctions() {
		File externalsFolder = new File("externals/metrics/");
		
		Collection<File> fileList = FileUtils.listFiles(externalsFolder, javaFilter, TrueFileFilter.TRUE);

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        MyDiagnosticListener c = new MyDiagnosticListener();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(c, Locale.ENGLISH,null);
        List<String> options = Arrays.asList("-d", "classes/","-source","1.8","-cp",System.getProperty("java.class.path")+"customfn/*");

		for(File file : fileList) {
            Iterable<? extends JavaFileObject> compilationUnit = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(file));
			JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager,c, options, null, compilationUnit);
			Boolean ok = task.call();
			
			if (ok){
				CustomCondition cc = new CustomCondition();
				List<String> lines = null;
				try {
					lines = Files.readAllLines(Paths.get(file.toURI()), Charset.defaultCharset());
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				StringBuilder sb = new StringBuilder();
				for (String s : lines){
					sb.append(s);
					sb.append(System.lineSeparator());
				}
				cc.setFullClassName(extractPkgName(sb.toString())+"."+extractClassName(sb.toString()));
				cc.setClassName(extractClassName(sb.toString()));
				customConditions.put(extractClassName(sb.toString()), cc);
			} else {
				System.out.println("Could not load Custom Function: "+file.getName());
			}
		}
	}
	
	
    private static String extractPkgName(final String source)
    {
        final Matcher m = PACKAGE_PATTERN.matcher(source);
        return m.find() ? m.group(1) : null;
    }

    private static String extractClassName(final String source)
    {
        final Matcher m = CLASSNAME_PATTERN.matcher(source);
        return m.find() ? m.group(1) : null;
    }

	
	private Tuple parse(URI lqmFile) throws IOException, ParseException{
		List<String> lines = Files.readAllLines(Paths.get(lqmFile), Charset.defaultCharset());
		StringBuilder sb = new StringBuilder();
		for (String s : lines){
			sb.append(s);
			sb.append(System.lineSeparator());
		}
		
		Reader reader = new StringReader(sb.toString()) ;
	    LQMLParser parser = new LQMLParser(reader) ;
	    parser.setCustomConditions(this.customConditions);
	    
	    Map<String, String> parse = parser.parse();
	    
	    String _jClass = this.javaClass.toString();
	    
	    _jClass = _jClass.replace("[%%packagename%%]", parse.get("[%%packagename%%]"));
	    _jClass = _jClass.replace("[%%metricuri%%]", parse.get("[%%metricuri%%]"));
	    _jClass = _jClass.replace("[%%imports%%]", parse.get("[%%imports%%]"));
	    _jClass = _jClass.replace("[%%author%%]", parse.get("[%%author%%]"));
	    _jClass = _jClass.replace("[%%label%%]", parse.get("[%%label%%]"));
	    _jClass = _jClass.replace("[%%description%%]", parse.get("[%%description%%]"));
	    _jClass = _jClass.replace("[%%classname%%]", parse.get("[%%classname%%]"));
	    _jClass = _jClass.replace("[%%variables%%]", parse.get("[%%variables%%]"));
	    _jClass = _jClass.replace("[%%computefunction%%]", parse.get("[%%computefunction%%]"));
	    _jClass = _jClass.replace("[%%metricvaluefuntion%%]", parse.get("[%%metricvaluefuntion%%]"));
	
	    
	    Tuple t = new Tuple();
	    t.className = parse.get("[%%packagename%%]") + "." +parse.get("[%%classname%%]");
	    t.javaClass = _jClass;
	    
	    return t;
	}
	
	private Set<URI> loadMetrics() throws IOException, ParseException{
		Set<URI> files = new HashSet<URI>(); 
		File externalsFolder = new File("externals/metrics/");
		
		Collection<File> fileList = 
				FileUtils.listFiles(externalsFolder, lqmFilter, TrueFileFilter.TRUE);
		
		for(File file : fileList) {
			files.add(file.toURI());
		}
		
		return files;
	}
	
	private class Tuple {
		protected String className;
		protected String javaClass;
	}
	
	
	
	// taken from: http://www.beyondlinux.com/2011/07/20/3-steps-to-dynamically-compile-instantiate-and-run-a-java-class/
	private static class MyDiagnosticListener implements DiagnosticListener<JavaFileObject> {
        public void report(Diagnostic<? extends JavaFileObject> diagnostic)
        {
            logger.warn("Line Number->" + diagnostic.getLineNumber());
            logger.warn("code->" + diagnostic.getCode());
            logger.warn("Message->"+ diagnostic.getMessage(Locale.ENGLISH));
            logger.warn("Source->" + diagnostic.getSource());
        }
    }
	
	 /** java File Object represents an in-memory java source file <br>
     * so there is no need to put the source file on hard disk  **/
    private static class InMemoryJavaFileObject extends SimpleJavaFileObject {
        private String contents = null;
 
        public InMemoryJavaFileObject(String className, String contents) throws Exception
        {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.contents = contents;
        }
 
        public CharSequence getCharContent(boolean ignoreEncodingErrors)
                throws IOException
        {
            return contents;
        }
    }
}
