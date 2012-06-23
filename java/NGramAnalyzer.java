import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;


public class NGramAnalyzer extends Analyzer {
	
	private Set <?> stopwords;
	private Version version;
	
	public NGramAnalyzer(Version matchVersion, Set<?> stopWords) {
	    version = matchVersion;
	    stopwords = stopWords;
	}

	/**
	 * Tokenization before
	 * apostrophe/hyphen removal before
	 * lower case before
	 * stop word filtering before
	 * n-gram creation before
	 * another round of stop word filtering, which is last
	 */
	@SuppressWarnings("deprecation")
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return //stop filtering happens twice so I can remove trigrams where it would be
				//inappropriate to remove the individual words
				//e.g. "my chemical romance" or "taking back sunday" or "green day"
				new StopFilter(
						version, 
						new ShingleMatrixFilter(
							new StopFilter(
									version, 
									new LowerCaseFilter(
											version,
											new StandardFilter(
													version,
													new StandardTokenizer(version, reader)
													)
										), 
									stopwords),
							1,3), //uni to tri
					stopwords);
	}
}
