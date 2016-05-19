from pyspark import SparkContext
import pysam,os

sc = SparkContext()
PATH = "/home/ubuntu/genome_project/spark/bam_files/"

def bamFiles():
        bamUrl = os.listdir("/home/ubuntu/genome_project/spark/bam_files")
        #distFiles = sc.parallelize(bamUrl)
        a = bamUrl[:2]
        distFiles = sc.parallelize(a)

	result = distFiles.foreach(lambda file: runFile(file))
        reduceKmers()

def runFile(file):
        samfile = pysam.AlignmentFile(PATH+file, "rb")
        kmer = 10
        kmer_list = []
        for r in samfile.fetch(until_eof=True):
                if r.is_unmapped:
                        test = r.query_alignment_sequence
                        for x in range(len(test)+1 - kmer):
                                kmers = test[x:x+kmer]
                                kmer_list.append(kmers)
                                with open("/home/ubuntu/genome_project/spark/txt_files/"+ file[:7]+".txt", "a") as f:
                                                f.write(str(kmers) + "\n")
        print len(kmer_list)

def reduceKmers():
        text_file = sc.textFile("/home/ubuntu/genome_project/spark/txt_files/*")
        counts = text_file.flatMap(lambda line: line.split("\n")) \
                                .map(lambda word: (word, 1)) \
                                .reduceByKey(lambda a, b: a + b)
        counts.saveAsTextFile("/home/ubuntu/genome_project/spark/omgwow.txt")
        samfile.close()

bamFiles()

