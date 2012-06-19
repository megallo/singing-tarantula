
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Creates a Lucene index from a directory of text files.
 * Notably loads stopwords from the supplied directory, and
 * uses Term Vectors so we can create TFIDF vectors with Mahout
 */

public class SongIndexer {

    public static final String CONTENT = "contents";
    public static final String DOCID = "docID";

    private IndexWriter writer;
    private ArrayList<File> queue = new ArrayList<File>();
    private Set<String> stopwords = new HashSet<String>();

    /**
     * 
     * @param args - <index output directory> <stopword input directory> <one or more input directories to index>
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        SongIndexer indexer = null;
        try {
            indexer = new SongIndexer();
            // load stop words before creating index; we need them for the analyzer
        	indexer.loadStopWords(new File(args[1]));
        	// now create index location
        	indexer.openIndex(args[0]);
        } catch (FileNotFoundException ex) {
        	printErrorAndExit("Problem loading stop words", ex);
        } catch (IOException ex) {
        	printErrorAndExit("Problem opening write location for index", ex);
        } catch (Exception ex) {
        	printErrorAndExit("Something broke", ex);
        }

        // process the files and folders that were listed in command line call
        for (int x = 2; x < args.length; x++) {
            try {
                indexer.indexFileOrDirectory(args[x]);
            } catch (Exception e) {
                System.out.println("Error indexing " + args[x]);
                e.printStackTrace();
            }
        }

        // close indexer so it will create the index
        indexer.closeIndex();
    }

    public void openIndex(String indexDir) throws IOException {
        FSDirectory dir = FSDirectory.open(new File(indexDir));

        NGramAnalyzer analyzer = new NGramAnalyzer(Version.LUCENE_34, stopwords);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, analyzer);

        writer = new IndexWriter(dir, config);
    }

    /**
     * Indexes a file or directory
     * @param fileName the name of a text file or a folder we wish to add to the index
     * @throws java.io.IOException
     */
    public void indexFileOrDirectory(String fileName) throws IOException {
        addFiles(new File(fileName));
        
        int originalNumDocs = writer.numDocs();
        for (File f : queue) {
                Document doc = new Document();
                String fileContents = readFile(f);
                if (fileContents.trim().isEmpty()) {
                	System.out.println("Skipping empty file " + f.getName());
                	continue;
                }
                
                // add contents of file
                // analyze the contents by tokenizing it
                doc.add(new Field(CONTENT, fileContents, Field.Store.YES, Field.Index.ANALYZED, TermVector.YES));
                
                // add docID, don't use it for queries
                doc.add(new Field(DOCID, f.getName(), Field.Store.YES, Field.Index.NOT_ANALYZED));

                writer.addDocument(doc);
                // System.out.println("Added: " + f);
        
        } // foreach file in queue

        int newNumDocs = writer.numDocs();
        System.out.println("");
        System.out.println("*************************");
        System.out.println((newNumDocs - originalNumDocs) + " documents added.");
        System.out.println("*************************");

        queue.clear();
    }

    /**
     * Dump the contents of one file to a String
     * @param pathname
     * @return
     * @throws IOException
     */
    private String readFile(File file) throws IOException {
        StringBuilder fileContents = new StringBuilder((int)file.length());
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while(scanner.hasNextLine()) {        
                fileContents.append(scanner.nextLine() + lineSeparator);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    /**
     * Adds either files to the queue directly , or if given a folder adds the contents of that folder.
     */
    private void addFiles(File file) {
        if (!file.exists()) {
            System.out.println(file + " does not exist.");
        }

        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                addFiles(f);
            }
        } else {
            queue.add(file);
        }
    }
    
    /**
     * Adds either files to the queue directly , or if given a folder adds the contents of that folder.
     * @throws FileNotFoundException 
     */
    private void loadStopWords(File file) throws FileNotFoundException {
        if (!file.exists()) {
            System.out.println(file + " does not exist.");
        }

        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
            	loadStopWords(f);
            }
        } else {
        	Scanner s = new Scanner(file);
        	String word;
        	while(s.hasNextLine()) {
        		if (!(word = s.nextLine()).trim().equals(""))
        			stopwords.add(word);
        	}
        	s.close();
        }
    }

    /**
     * Close the index.
     * @throws java.io.IOException
     */
    public void closeIndex() throws IOException {
        writer.close();
    }
    
    public static void printErrorAndExit(String message, Exception ex) {
        System.out.println(message);
        ex.printStackTrace();
        System.exit(1);
    }

}