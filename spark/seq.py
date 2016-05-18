from pyspark import SparkContext
sc = SparkContext()

import pysam
bamUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
with pysam.AlignmentFile(bamUrl,"rb") as samfile:
        kmer = 10
        kmer_list = []
        for r in samfile.fetch(until_eof=True):
                if r.is_unmapped:
                        test = r.query_alignment_sequence
                        for x in range(len(test)+1 - kmer):
                                kmers = test[x:x+kmer]
                                kmer_list.append(kmers)
                                with open("/home/ubuntu/genome_project/spark/output.txt", "a") as f:
                                       f.write(str(kmers) + "\n")
        print len(kmer_list)
        text_file = sc.textFile("/home/ubuntu/genome_project/spark/output.txt")
        counts = text_file.flatMap(lambda line: line.split("\n")) \
        			.map(lambda word: (word, 1)) \
        			.reduceByKey(lambda a, b: a + b)
        counts.saveAsTextFile("/home/ubuntu/genome_project/spark/omgwow.txt")
        samfile.close()
