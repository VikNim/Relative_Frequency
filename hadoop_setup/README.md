# Relative-frequencies-of-words

Developed a Hadoop based MapReduce program to compute the relative frequencies of each word that occurs in all the documents in 100KWikiText.txt, and output the top 100 word pairs sorted in a decreasing order of relative frequency. 

Relative frequency of word B given word A is defined as follows:

    f(B|A) = count(A,B)/count(A)
    
where count(A,B) is the number of times A and B co-occur in a document, and count(A) the number of times A occurs with anything else. Intuitively, given a document collection, the relative frequency captures the proportion of time the word B appears in the same document as A.
