package common;

import org.apache.hadoop.fs.Path;

public class PathUtil {
	
	//phase 1: offline TFIDF calculation
	public final static Path OUTPUT_PATH_UNI = new Path("/output/unigramLocal");
	public final static Path OUTPUT_PATH_MAX = new Path("/output/maxLocal");
	public final static Path OUTPUT_PATH_NI = new Path("/output/niLocal");
	public final static Path OUTPUT_PATH_N = new Path("/output/NLocal");
	public final static Path OUTPUT_PATH_OFFLINE_RESULT = new Path("/output/local1");
	/* /*
	public final static Path OUTPUT_PATH_UNI = new Path("/home/output/unigram");
	public final static Path OUTPUT_PATH_MAX = new Path("/home/output/max");
	public final static Path OUTPUT_PATH_NI = new Path("/home/output/ni");
	public final static Path OUTPUT_PATH_N = new Path("/home/output/N");
	*/
	
	//phase 2 calculate unknown doc TFIDF
	public final static Path OUTPUT_PATH_UNKNOWN_UNI = new Path("/output/unknownUnigramLocal");
	public final static Path OUTPUT_FILE_UNKNOWN_UNI = new Path("/output/unknownUnigramLocal/part-r-00000");
	public final static Path OUTPUT_FILE_UNKNOWN_TF = new Path("/output/unknownTFLocal");
	public final static Path OUTPUT_PATH_UNKNOWN_TFIDF = new Path("/output/unknownTFIDFLocal");
	public final static Path OUTPUT_FILE_UNKNOWN_TFIDF = new Path("/output/unknownTFIDFLocal/part-r-00000");
	public final static Path OUTPUT_PATH_FINAL = new Path("/output/finalLocal");
}
