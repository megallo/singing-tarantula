import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
	private static double TOO_SMALL = 1e-12;

	/**
	 * @param args - <vector file> <dictionary file> [minimum weight to show]
	 */
	public static void main(String[] args) {
		VectorReader v = new VectorReader();
		try {
			v.loadDictionary(args[1]);
			if (args.length > 2)
				v.loadVectors(args[0], Double.valueOf(args[2]));
			else
				v.loadVectors(args[0], TOO_SMALL);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void loadVectors(String vectorsPath, double min) throws IOException {
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
				if (e.get() > TOO_SMALL && e.get() > min)
										//token						//tf-idf weight
					System.out.println(dictionaryMap.get(e.index()) + "\t\t" + e.get()) ;
			}
		}
		reader.close();
	}
	
	public void loadDictionary(String dictionaryPath) throws IOException {
		Scanner s = new Scanner(new File(dictionaryPath));
		s.nextLine(); s.nextLine(); //skip count and header
		while (s.hasNextLine()) {
			String line = s.nextLine();
			String [] tokens = line.split("\\s+");
			try {
				dictionaryMap.put(Integer.parseInt(tokens[2]), tokens[0]);
			} catch (ArrayIndexOutOfBoundsException e) { }
		}
		s.close();
	}
}
