import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;


public class NGramAnalyzer extends Analyzer {
	
	private Set <?> stopwords;
	private Version version;
	
	public NGramAnalyzer(Version matchVersion, Set<?> stopWords) {
	    version = matchVersion;
	    stopwords = stopWords;
	}

	@SuppressWarnings("deprecation")
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new StopFilter(version,
				new LowerCaseFilter(
						new ShingleMatrixFilter(
								new StandardTokenizer(version, reader),
								1,3) //uni to tri
						),
	           stopwords);
	}

}
