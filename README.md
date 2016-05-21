# [MapReduce] Content Based Authorship Detection

This project is for CS435 Big data Spring 2016. It builds an authorship detection system that provides a ranked list of possible authors for a document whose authorship is unknown. 

## Steps:

* calculate TFIDF values of each unigram for all of the sub-collections in the given corpus.
* create the attribute vectors for every author and store the results in a HDFS file
* Read a document with unknown authorship and create an attribute vector for it
* calculate the Cosine Similarity between the author attribute vector for this document and all of the author attribute vectors calculated before
* select top 10 authors


