package any23tool.any23tool;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.writer.TripleHandlerException;
import org.junit.Test;


public class Any23ToolTest {
	
	@Test
	public void test(){
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available logical cores: "+cores);
	}

	@Test
	public void rdfFilestest() throws Exception {
		final Any23 runner = new Any23();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available logical cores: "+cores);
		final ExecutorService executor = Executors.newFixedThreadPool(cores);
		Converter c = new Converter(runner, executor);

		File output = new File("target/test.nq");
		
		runOnce(c, new File("src/test/resources/wordnet-participleof.rdf"), output);
		runOnce(c, new File("src/test/resources/VulneraPedia.full.v0.5.rdf"), output);
	}

	@Test
	public void csvFilestest() throws Exception {
		final Any23 runner = new Any23();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available logical cores: "+cores);
		final ExecutorService executor = Executors.newFixedThreadPool(cores);
		Converter c = new Converter(runner, executor);

		File output = new File("target/test.nq");
		
		runOnce(c, new File("src/test/resources/small.csv"), output);
	}

	
	private void runOnce(Converter c, File input, File output) throws FileNotFoundException,
			IOException, ExtractionException, TripleHandlerException {
		
		if(output.exists()){
			output.delete();
		}
		
		c.convert(input, output, "nquads", "http://example.com");
		assertTrue(output.exists());
		assertTrue(output.length() > 0);

		if(output.exists()){
			output.delete();
		}
	}
}