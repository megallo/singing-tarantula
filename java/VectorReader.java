import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * Reads vectors from a file
 * 
 * Blatantly stolen from 
 * http://www.philippeadjiman.com/blog/2010/12/30/how-to-easily-build-and-observe-tf-idf-weight-vectors-with-lucene-and-mahout/
 * 
 */

public class VectorReader {
	HashMap<Integer, String> dictionaryMap = new HashMap<Integer, String>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		VectorReader v = new VectorReader();
		try {
			v.loadDictionary();
			v.loadVectors(args[0]);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void loadVectors(String vectorsPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(vectorsPath);
		 
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		LongWritable key = new LongWritable();
		VectorWritable value = new VectorWritable();
		while (reader.next(key, value)) {
			NamedVector namedVector = (NamedVector)value.get();
			RandomAccessSparseVector vect = (RandomAccessSparseVector)namedVector.getDelegate();
			System.out.println("docID: " + namedVector.getName());
			for( Element  e : vect ){
				System.out.println("Token: " + dictionaryMap.get(e.index()) + "\tTF-IDF weight: " + e.get()) ;
			}
		}
		reader.close();
	}
	
	public void loadDictionary() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader read = new SequenceFile.Reader(fs, new Path(""), conf);
		IntWritable dicKey = new IntWritable();
		Text text = new Text();
		
		while (read.next(text, dicKey)) {
			dictionaryMap.put(Integer.parseInt(dicKey.toString()), text.toString());
		}
		read.close();
	}
}
