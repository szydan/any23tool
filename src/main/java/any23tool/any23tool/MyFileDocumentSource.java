package any23tool.any23tool;

import java.io.File;

import org.apache.any23.source.FileDocumentSource;

/**
 * Extended FileDocumentSource which allow to force the contentType 
 * which is not possible in original implementation
 * 
 * Note: Do not be tempted to use another source as all others put the data into memory 
 * making it unusable for large files
 * 
 * @author szydan
 *
 */
public class MyFileDocumentSource extends FileDocumentSource {

	private String contentType;

	public MyFileDocumentSource(File file) {
		super(file);
	}

    /**
     * Constructor to force contentType 
     * 
     * @param file
     * @param baseURI
     * @param contentType
     */
    public MyFileDocumentSource(File file, String baseURI, String contentType) {
    	super(file,baseURI);
    	this.contentType = contentType;
    }
	
	@Override
	public String getContentType(){
		return this.contentType;
	}
	
}
