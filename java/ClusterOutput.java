import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

public class ClusterOutput {

/**
 * @param args - input clusteredPoints dir, output vectors, output cluster IDs
 */
public static void main(String[] args) {
    try {
        BufferedWriter bw;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        File pointsFolder = new File(args[0]);
        File files[] = pointsFolder.listFiles();
        bw = new BufferedWriter(new FileWriter(new File(args[1])));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(args[3])));
        HashMap<String, Integer> clusterIds;
        clusterIds = new HashMap<String, Integer>(5000);
        for(File file:files){
            if(!file.getName().equals("part-m-00000"))
                continue;
            System.out.println("reading file " + file.getName());
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,  new Path(file.getAbsolutePath()), conf);
            IntWritable key = new IntWritable();
            WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
            while (reader.next(key, value)) {
            	//value.write(out);
            	String vectorName;
            	Vector vector = value.getVector();
            	if (vector instanceof NamedVector) {
            	    vector = (NamedVector) value.getVector();
                    vectorName = ((NamedVector) vector).getName();
            	} else  {
	                vectorName = vector.asFormatString();
            	}
                bw.write(vectorName + "\t" + key.toString()+"\n");
                if(clusterIds.containsKey(key.toString())){
                    clusterIds.put(key.toString(), clusterIds.get(key.toString())+1);
                }
                else
                    clusterIds.put(key.toString(), 1);
            }
            bw.flush();
            reader.close(); 
        }
        bw.flush();
        bw.close();
        bw = new BufferedWriter(new FileWriter(new File(args[2])));
        Set<String> keys=clusterIds.keySet();
        for(String key:keys){
            bw.write(key+" "+clusterIds.get(key)+"\n");
        }
        bw.flush();
        bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
