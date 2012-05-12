import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;


/**
 * I got tired of googling for how to create Writable Cluster Vectors using 
 * Mahout command line, so I just hacked RandomSeedGenerator until it
 * fit my purposes
 * 
 * This is specific to K-Means clusters, btw
 * 
 * Also I took out the config stuff so I have no idea if it will run on Hadoop fs files
 *
 */
public class BuildSeedClusters {

	/**
	 * @param args <input dir containing sequence files> <output dir> <DistanceMeasure>
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Args: <input dir containing sequence files> <output dir> <DistanceMeasure>");
			System.exit(1);
		}
		try {
			DistanceMeasure dm = (DistanceMeasure) Class.forName(args[2]).newInstance();
			BuildSeedClusters.buildSeeds(new Path(args[0]), new Path(args[1]), dm);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Path buildSeeds(Path input, Path output, DistanceMeasure measure) throws IOException {
		Configuration conf = new Configuration(); //things I don't care about right now
		// delete the output directory
		FileSystem fs = FileSystem.get(output.toUri(), conf);
		HadoopUtil.delete(conf, output);
		Path outFile = new Path(output, "part-notrandomSeed");
		boolean newFile = fs.createNewFile(outFile);
		if (newFile) {
			Path inputPathPattern;

			if (fs.getFileStatus(input).isDir()) {
				inputPathPattern = new Path(input, "*");
			} else {
				inputPathPattern = input;
			}

			FileStatus[] inputFiles = fs.globStatus(inputPathPattern, PathFilters.logsCRCFilter());
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outFile, Text.class, Cluster.class);
			List<Text> chosenTexts = new ArrayList<Text>();
			List<Cluster> chosenClusters = new ArrayList<Cluster>();
			int nextClusterId = 0;

			for (FileStatus fileStatus : inputFiles) {
				if (fileStatus.isDir()) {
					continue;
				}
				//iterate over vectors and add them to our cluster output
				for (Pair<Writable, VectorWritable> record : new SequenceFileIterable<Writable, VectorWritable>(
						fileStatus.getPath(), true, conf)) {
					Writable key = record.getFirst();
					VectorWritable value = record.getSecond();
					Cluster newCluster = new Cluster(value.get(), nextClusterId++, measure);
					newCluster.observe(value.get(), 1);
					Text newText = new Text(key.toString());
					chosenTexts.add(newText);
					chosenClusters.add(newCluster);
				}
			}

			try {
				for (int i = 0; i < chosenTexts.size(); i++) {
					writer.append(chosenTexts.get(i), chosenClusters.get(i));
				}
				System.out.printf("Wrote {} vectors to {} \n", chosenTexts.size(), outFile);
			} finally {
				writer.close();
			}
		}
		return outFile;
	}
}
