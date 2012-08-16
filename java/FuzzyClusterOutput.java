import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

/**
 * Adaptation of cluster writer from Mahout in Action,
 * but fixed for 0.6 and writes more useful cluster info,
 * and then re-adapted to work with fuzzy cluster probabilities
 * 
 * Output file name is the user-defined prefix_alldocs
 * 
 * @author megs
 */
public class FuzzyClusterOutput {
	
	//holds the name of the point mapped to a list of pairs of cluster-probabilities
	private HashMap<String, ArrayList<Pair>> pointVectorToClusterProbs = new HashMap<String, ArrayList<Pair>>();
	
	private String inputDir;

/**
 * @param args - <input clusteredPoints dir>  <output file prefix>
 */
	public static void main(String[] args) {
	    try {
	    	FuzzyClusterOutput fco = new FuzzyClusterOutput(args[0]);
	    	fco.doStuff(args[1]);
	        } catch (IOException e) {
	            e.printStackTrace();
        }
    }

	public FuzzyClusterOutput(String clusteredPointsDir) {
		inputDir = clusteredPointsDir;
	}

	public void doStuff(String outputFilePrefix) throws IOException {

        BufferedWriter bw;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        File pointsFolder = new File(inputDir);

        for (File file : pointsFolder.listFiles()) {
            if(!file.getName().startsWith("part-"))
                continue;
            System.out.println("reading file " + file.getAbsolutePath());
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,  new Path(file.getAbsolutePath()), conf);
            System.out.println("key is class " + reader.getKeyClassName() + " and value is class " + reader.getValueClassName());
            IntWritable key = new IntWritable();
            WeightedVectorWritable value = new WeightedVectorWritable();

            // read all the pairs in the sequence file and put them in a map
            while (reader.next(key, value)) {
            	//key is the cluster ID, value is the vector mapping clusters to probabilities for this document
            	String vectorName;
            	Vector vector = value.getVector();
            	if (vector instanceof NamedVector) {
            	    vector = (NamedVector) value.getVector();
                    vectorName = ((NamedVector) vector).getName();
            	} else  {
            		//just so it doesn't break if you didn't use named vectors
	                vectorName = vector.asFormatString();
            	}
            	
            	//add to map
            	insertOrAppend(vectorName, key.toString(), value.getWeight());
            }
            reader.close();
        }
        
        /** finished reading. now write to file **/
    	bw = new BufferedWriter(new FileWriter(new File(outputFilePrefix+"_alldocs")));
        
        for (String key : pointVectorToClusterProbs.keySet()) {
        	ArrayList<Pair> clusterProbs = pointVectorToClusterProbs.get(key);
        	Collections.sort(clusterProbs); //sorts descending by probability
        	bw.write(key + "\n");
        	for (Pair cluster : clusterProbs)
        		bw.write(cluster.word + "\t" + cluster.similarity + "\n");
        	bw.write("\n"); //finished with one point
        }
        bw.flush();
        bw.close();
	}

	private void insertOrAppend(String vectorName, String clusterID, double probability) {
		Pair pair = new Pair(clusterID, probability);
		if (pointVectorToClusterProbs.containsKey(clusterID)) {
			pointVectorToClusterProbs.get(clusterID).add(pair);
		} else {
			ArrayList<Pair> addme = new ArrayList<Pair>();
			addme.add(pair);
			pointVectorToClusterProbs.put(clusterID, addme);
		}
	}

	/**
	 * A string and its number, to make it easier to sort
	 * Sorts by numeric descending
	 */
	private class Pair implements Comparable<Object> {
		String word;
		double similarity;
		
		Pair (String word, double distance) {
			this.word = word;
			this.similarity = distance;
		}

		public boolean equals(Object other) {
			if (((Pair) other).word.equals(this.word) && 
					((Pair) other).similarity == (this.similarity))
				return true;
			else
				return false;
		}
		
		public int hashCode() {
		    int hash = 1;
		    hash = hash * 17 + word.hashCode();
		    hash = hash * 17 + Double.valueOf(similarity).hashCode();
		    return hash;
		}
		
		//Sorts descending by similarity
		public int compareTo(Object that) {
			if (this.similarity < ((Pair)that).similarity)
				return 1;
			else if (this.similarity == ((Pair)that).similarity)
				return 0;
			else
				return -1;
		}
		
		public String toString() {
			return "[" + word + ", " + similarity + "]";
		}
	}
}
