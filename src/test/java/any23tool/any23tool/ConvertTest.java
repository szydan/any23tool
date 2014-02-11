package any23tool.any23tool;

import org.junit.Test;

public class ConvertTest {
	
	@Test
	public void test(){
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available logical cores: "+cores);
	}
}