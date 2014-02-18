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
import org.apache.any23.source.DocumentSource;
import org.apache.any23.writer.NQuadsWriter;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;

/**
 * Use
 * 
 * java -jar any23tool.jar /input/communes.ttl /output/communes.nq
 * 
 */
public class Any23Tool {
	
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
		final ExecutorService executor = Executors.newFixedThreadPool(cores);
		
		Converter c = new Converter(runner, executor);
		c.convert(new File(inputFileFolderPath), new File(outputFileFolderPath), format, graphUri);
	}

	

}
