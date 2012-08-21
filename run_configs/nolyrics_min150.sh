### This needs to be run from the top level project directory, and you'll need the java classes on the classpath, and mahout on the path

INPUT="raw"
OUTPUT="nolyrics_min150"
K=12
ITERATIONS=100
NGRAMS=3
MIN_DOC_FREQ=10
MAX_DOC_PERCENTAGE=80

# -xs is TF sigma (std dev). for removing high freq terms
# -n is normalization lp. 2 is good for cosine distance

# create lucene index
java SongIndexer index stopwords data/${INPUT}

# create vectors from lucene index
mahout lucene.vector --dir index --idField docID -o data/vectors/tfidf-vectors --field contents --dictOut data/vectors/dict.txt -n 2 --maxDFPercent ${MAX_DOC_PERCENTAGE} -w TFIDF --minDF ${MIN_DOC_FREQ}

#dump top terms for documents into a file
java VectorReader data/vectors/tfidf-vectors data/vectors/dict.txt .05 > cluster_results/docid_weight

# run kmeans
mahout kmeans -i data/vectors/tfidf-vectors  -c ./clusters -o mahout_output/${OUTPUT}/k-means-results -x ${ITERATIONS} -k ${K} -ow -cl --distanceMeasure org.apache.mahout.common.distance.CosineDistanceMeasure

# write cluster docs to files
java ClusterOutput mahout_output/${OUTPUT}/k-means-results/clusteredPoints/ cluster_results/${OUTPUT}

# write cluster top terms to file
mahout clusterdump -s mahout_output/${OUTPUT}/k-means-results/clusters-*-final --dictionary data/vectors/dict.txt --dictionaryType text -b 200 -n 44 -o cluster_results/${OUTPUT}_cluster_topterms.txt --distanceMeasure org.apache.mahout.common.distance.CosineDistanceMeasure

### the old way to make vectors, directly from text files. can't use custom stopwords this way
#mahout seqdirectory -i data/${INPUT} -o mahout_output/${OUTPUT}/sequence_files -c US-ASCII -chunk 64
#mahout seq2sparse -i mahout_output/${OUTPUT}/sequence_files -o mahout_output/${OUTPUT}/sparse_vectors -ow -nv -wt TFIDF -ng ${NGRAMS} -xs 3.0 -md ${MIN_DOC_FREQ} -n 2 
