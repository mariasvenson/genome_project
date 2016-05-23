from pyspark import SparkContext
import pysam, os 

sc = SparkContext()
PATH = "/home/ubuntu/genome_project/spark/bam_files/"
KMER_PATH = "/home/ubuntu/genome_project/spark/txt_files/"
#Find all kmers and save in file
def findKmers(file):
        samfile = pysam.AlignmentFile(PATH+file, "rb")
        kmer = 10
        kmer_list = []
        for r in samfile.fetch(until_eof=True):
                if r.is_unmapped:
                        test = r.query_alignment_sequence
                        for x in range(len(test)+1 - kmer):
                                kmers = test[x:x+kmer]
                                kmer_list.append(kmers)
                                with open(KMER_PATH+ file[:7]+".txt", "a") as f:
                                                f.write(str(kmers) + "\n")
        print len(kmer_list)
        samfile.close()
        return kmer_list

def filterKmer(tuple):
        if tuple[1] >= 10 and tuple[1] <= 200:
                return tuple
        else:
                return None

def bamFiles():
        bamUrl = os.listdir("/home/ubuntu/genome_project/spark/bam_files")
        #distFiles = sc.parallelize(bamUrl)
        bamFiles = bamUrl[:6]
        distFiles = sc.parallelize(bamFiles)

        kmer_res = distFiles.flatMap(lambda file: findKmers(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
        kmer_res.saveAsTextFile("output_RAW")

        kmer_range = kmer_res.map(lambda line: filterKmer(line)).filter(lambda obj: obj != None)
        kmer_range.saveAsTextFile("kmers_range")
        for obj in kmer_range.collect():
                with open("/home/ubuntu/genome_project/spark/EVERYTHING.txt", "a") as f:
                        f.write(str(obj) + "\n")


bamFiles()
