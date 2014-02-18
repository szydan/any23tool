package any23tool.any23tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.Extractor;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.writer.NQuadsWriter;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;

public class Converter {

	private final Any23 runner;
	private final ExecutorService executor;
	
	
	public Converter(Any23 runner, ExecutorService executor) {
		this.runner = runner;
		this.executor = executor;
	}

	public void convert(
			File inputFile, File outputFile,
			String format, String graphUri)
			throws FileNotFoundException, IOException, ExtractionException,
			TripleHandlerException {
		
		
		if( inputFile.isDirectory() && outputFile.isDirectory() ){

			File outputDir = outputFile;
			File inputDir = inputFile;
			
			File[] files = inputDir.listFiles(new FilenameFilter(){

				public boolean accept(File dir, String name) {
					if( name.startsWith(".")){
						return false;
					}
					return true;
				}
			});
			
			
			for(final File f : files){
				String outputPath = f.getName();
				int index = outputPath.lastIndexOf("."); 
				if(index!=-1){
					outputPath = outputPath.substring(0,index);
				}
				
				if(format.equals("nquads") ){
					outputPath+=".nq";
				}else{
					outputPath+=".nt";
				}
				final String graph = graphUri;
				final File out = new File(outputDir,outputPath);
				final String formatF = format;
				
				executor.execute(new Runnable(){

					public void run() {
						try {
							convertSingleFile(f,out, graph, formatF, null);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}});
			}
		}else{
			convertSingleFile(inputFile, outputFile, graphUri, format, null);
		}
		
		executor.shutdown();
		System.out.println("Done");
	}
	
	private void convertSingleFile(File  inputFile, File outputFile, String graphUri,
			String format, String inputContentType) throws FileNotFoundException,
			IOException, ExtractionException, TripleHandlerException {
		
		System.out.println("Converting\nfrom: "+inputFile.getAbsolutePath()+"\nto  : "+outputFile.getAbsolutePath());
		
		DocumentSource source = new MyFileDocumentSource(inputFile, graphUri, inputContentType);
		OutputStream out = new FileOutputStream(outputFile);
		
		TripleHandler handler = new NQuadsWriter(out);
		// TurtleWriter.java
		// RDFXMLWriter.java
		if(format.equals("ntriples")){
			handler = new NTriplesWriter(out);
		}
		
		ExtractionReport report;
		try {
			report = runner.extract(source, handler);
		} finally {
			handler.close();
			out.close();
		}


		// check the output file size 
		// 0 indicates something went wrong
		
		if( ! report.hasMatchingExtractors() || outputFile.length() == 0){
			System.out.println("Did not find matching extractor or produced file is empty");
			System.out.println("Detected mimeType: "+report.getDetectedMimeType());
			
			// here try to run it one more time with extractor based on file extension
			if(inputFile.getAbsolutePath().endsWith(".rdf") || inputFile.getAbsolutePath().endsWith(".xml")){
				System.out.println("Will try to do it again with rdf-xml extractor");
				convertSingleFile(inputFile,outputFile, graphUri, format, "application/rdf+xml"); 
			}
			
			
		}else{
			System.out.println("Found following matching extractors: ");
			for(Extractor extractor : report.getMatchingExtractors()){
				System.out.println(extractor.getDescription().getExtractorName());
			}
		}
	}

}
