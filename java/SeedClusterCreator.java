import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Reads in a SequenceFile of vectors and a file containing docIDs. Creates a
 * SequenceFile of clusters from the vectors matching the docIDs.
 * 
 * Currently compatibly with Mahout 0.7
 * 
 * @author megs
 */
public class SeedClusterCreator {

	private String inputVectorFile;
	private String seedFile;
	private Path outputDir;
	private Set<String> seedNames = new HashSet<String>();

	/**
	 * @param args
	 *            - <vector file> <seed id file> <output dir>
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("Usage:\njava SeedClusterCreator <vector file> <seed id file> <output dir>");
			System.exit(1);
		}

		try {
			SeedClusterCreator seeder = new SeedClusterCreator(args[0], args[1], args[2]);
			seeder.doStuff();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public SeedClusterCreator(String vectorFileName, String seedFileName,
			String outputDirName) {
		inputVectorFile = vectorFileName;
		outputDir = new Path(outputDirName);
		seedFile = seedFileName;
	}

	public void doStuff() throws IOException {
		readSeedIDs();
		readVectorFile();
	}

	/**
	 * Reads in a SequenceFile with NamedVectors and finds the seed vectors.
	 * Converts them to clusters and writes them to a Cluster SequenceFile
	 * 
	 * @throws IOException
	 */
	private void readVectorFile () throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        //set up for reading stuff
        File vectorfile = new File(inputVectorFile);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,  new Path(vectorfile.getAbsolutePath()), conf);
        LongWritable key = new LongWritable();
        VectorWritable value = new VectorWritable();

        //set up for writing stuff
		Path outFile = new Path(outputDir, "part-seeds");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				outFile, Text.class, Kluster.class);
		List<Text> chosenTexts = new ArrayList<Text>();
		List<Kluster> chosenClusters = new ArrayList<Kluster>();

		//set up stuff for new cluster creation
        int nextClusterId = 0;
        //hardcoding, lazy
        DistanceMeasure measure = ClassUtils.instantiateAs(CosineDistanceMeasure.class.getName(), DistanceMeasure.class);

        int count = 0;
        // read all the pairs in the sequence file and find the seed vectors
        while (reader.next(key, value)) {
        	//key is some kind of ID, value is the named vector holder
        	String vectorName;
        	Vector vector = value.get();
        	if (vector instanceof NamedVector) {
                vectorName = ((NamedVector) vector).getName();
        	} else  {
                continue;
                //you forgot the -nv flag
        	}
        	System.out.println("Found vector " + vectorName);
        	count++;
        	if (seedNames.contains(vectorName)) {
        		System.out.println("Found vector for desired seed " + vectorName);

				Kluster newCluster = new Kluster(vector, nextClusterId++, measure);
				//newCluster.observe(value.get(), 1); //?? why was I doing this
				Text newText = new Text(key.toString());

				//add to lists, and then we'll write them as key/value pairs all at once
				chosenTexts.add(newText);
				chosenClusters.add(newCluster);
        	}
        }
        reader.close();
        System.out.println("Total count " + count);
		try {
	        //we've collected all the vectors for the requested seeds
	        // if we couldn't find one, complain loudly and refuse to write the cluster file
	        if (chosenTexts.size() != seedNames.size()) {
	        	System.err.println("Sorry, I couldn't find all of the seeds you wanted. Giving up and going home.");
	        	System.exit(1);
	        }
			for (int i = 0; i < chosenTexts.size(); i++) {
				writer.append(chosenTexts.get(i), chosenClusters.get(i));
			}
			System.out.println("Wrote " + chosenTexts.size() + " vectors to " + outFile);
		} finally {
			if (writer != null) writer.close();
		}
    }
	
	/**
	 * Reads a text file. The seed names should match whatever you set as the name for your NamedVectors
	 * 
	 * @throws IOException
	 */
	private void readSeedIDs() throws IOException {
		Scanner s = new Scanner(new File(seedFile));
		while (s.hasNextLine()) {
			String id = s.nextLine().trim();
			if (!id.startsWith("#") && !id.isEmpty()) {
				seedNames.add(id);
			}
		}
		s.close();
	}
}
