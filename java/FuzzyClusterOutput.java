import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
 * but fixed for 0.6 and writes more useful cluster info
 * 
 * Output file name is the user-defined prefix_clusterID__count of items in cluster
 * 
 * @author megs
 */
public class FuzzyClusterOutput {
	
	private HashMap<String, ArrayList<String>> clusterIDConstituents = new HashMap<String, ArrayList<String>>();
	
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
            IntWritable key = new IntWritable();
            WeightedVectorWritable value = new WeightedVectorWritable();

            //TODO: figure out how to map the vector int id back to the name of the vector
            bw = new BufferedWriter(new FileWriter(new File(outputFilePrefix+"_alldocs")));
            // read all the pairs in the sequence file and put them in a map
            while (reader.next(key, value)) {
            	//key is the cluster ID, value is the vector mapping clusters to probabilities for this document
            	String vectorName;
            	Vector vector = value.getVector();
            	bw.write(key + "\n");
            	if (vector instanceof NamedVector) {
            	    vector = (NamedVector) value.getVector();
                    vectorName = ((NamedVector) vector).getName();
            	} else  {
            		//just so it doesn't break if you didn't use named vectors
	                vectorName = vector.asFormatString();
            	}
            	
            	bw.write(vectorName + "\t" + value.getWeight() + "\n\n");
//            	//add to map
//            	insertOrAppend(key.toString(), vectorName);
            }
            reader.close();
            bw.flush();
            bw.close();
        }
        
        /** finished reading. now write to files **/
        
//        //for each cluster, write a separate file
//        for (String key : clusterIDConstituents.keySet()) {
//        	ArrayList<String> vectornames = clusterIDConstituents.get(key);
//        	//file name is the user-defined prefix, cluster ID, count of items in cluster
//        	bw = new BufferedWriter(new FileWriter(new File(outputFilePrefix+"_" + key + "__" + vectornames.size())));
//        	//write the items in the cluster 
//        	for (String name : vectornames) {
//        		bw.write(name + "\n");
//        	}
//	        bw.flush();
//	        bw.close();
//        }
	}

	private void insertOrAppend(String clusterID, String name) {
		if (clusterIDConstituents.containsKey(clusterID)) {
			clusterIDConstituents.get(clusterID).add(name);
		} else {
			ArrayList<String> addme = new ArrayList<String>();
			addme.add(name);
			clusterIDConstituents.put(clusterID, addme);
		}
	}
}
