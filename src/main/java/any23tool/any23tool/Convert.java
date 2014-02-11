package any23tool.any23tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.Extractor;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.rdf.RDFXMLExtractor;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.FileDocumentSource;
import org.apache.any23.writer.NQuadsWriter;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/**
 * Use
 * 
 * java -jar any23tool.jar /input/communes.ttl /output/communes.nq
 * 
 */
public class Convert {
	
	public static void main(String[] args) throws IOException,
			ExtractionException, TripleHandlerException {

		if(args.length < 2){
			System.out.println("Try:");
			System.out.println("java -jar any23tool.jar inputFilePath outputFilePath");
			System.out.println("java -jar any23tool.jar inputFilePath outputFilePath [nquads,ntriples] [graphURI]");
			
			System.out.println("Where:");
			System.out.println("inputFilePath and outputfilePath are either path to single files or paths to directories");
			
			System.exit(-1);
		}
		String graphUri = "http://example.com";
		String format = "ntriples";
		
		//TODO assume the output format based on file extension
		if(args.length > 2 && "nquads".equals(args[2])){
			format = "nquads";
		}
		if(args.length == 4 ){
			graphUri = args[3];
		}
		
		String inputFileFolderPath = args[0];
		String outputFileFolderPath = args[1];
		
		
		
		
		final Any23 runner = new Any23();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available logical cores: "+cores);
		ExecutorService executor = Executors.newFixedThreadPool(cores);
		
		
		if( (new File(inputFileFolderPath).isDirectory()) && (new File(outputFileFolderPath).isDirectory()) ){

			File inputDir = new File(inputFileFolderPath);
			File[] files = inputDir.listFiles(new FilenameFilter(){

				public boolean accept(File dir, String name) {
					if( name.startsWith(".")){
						return false;
					}
					return true;
				}
			});
			
			File outputDir = new File(outputFileFolderPath);
			
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
							convertSingleFile(f.getAbsolutePath(),out.getAbsolutePath(), graph, formatF, runner, null);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}});
			}
		}else{
			convertSingleFile(inputFileFolderPath,outputFileFolderPath, graphUri, format, runner, null);
		}
		
		executor.shutdown();
		System.out.println("Done");
	}

	private static void convertSingleFile(String  inputPath,String outputPath, String graphUri,
			String format, Any23 runner, String inputContentType) throws FileNotFoundException,
			IOException, ExtractionException, TripleHandlerException {
		
		System.out.println("Converting\nfrom: "+inputPath+"\nto  : "+outputPath);
		
		DocumentSource source = new MyFileDocumentSource(new File(inputPath),graphUri, inputContentType);
		OutputStream out = new FileOutputStream(outputPath);
		
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


		if( ! report.hasMatchingExtractors() ){
			System.out.println("Did not find matching extractor");
			System.out.println("Detected mimeType: "+report.getDetectedMimeType());
			
			// here try to run it one more time with extractor based on file extension
			if(inputPath.endsWith(".rdf") || inputPath.endsWith(".xml")){
				System.out.println("Will try to do it again with rdf-xml extractor");
				convertSingleFile(inputPath,outputPath, graphUri, format, runner, "application/rdf+xml"); 
			}
			
			
		}else{
			System.out.println("Found following matching extractors: ");
			for(Extractor extractor : report.getMatchingExtractors()){
				System.out.println(extractor.getDescription().getExtractorName());
			}
		}
	}
}
