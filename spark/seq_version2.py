from pyspark import SparkContext
import pysam,os, sys

sc = SparkContext()
PATH = "/home/ubuntu/genome_project/spark/bam_files/"
KMER_PATH = "/home/ubuntu/genome_project/spark/txt_files/"
HEAT = 	    "/home/ubuntu/genome_project/spark/heat/"
KUL =       "/home/ubuntu/genome_project/spark"
KMER =      "/home/ubuntu/genome_project/spark/kmer/"
#Find all kmers and save in file

#def findPosition(heat_list):
#	reduced_list = []
#	for x in heat_list: 
#		pos = str(x)
#		red_pos = pos[3:]
#		reduced_list.append(red_pos)
#	#.reduceByKey(lambda a,b: a+b)
#        	with open(HEAT+ "list" + ".txt", "a") as f:
#                       	f.write(red_pos + "\n")

        #mappa alla positioner som ligger i mappen  

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
                                #with open(KMER+ file[:7]+".txt", "a") as f:
                                #                f.write(str(kmers) + "\n")

        print "******** WORKING ON: " + file + "  WITH LENGTH:" + str(len(kmer_list)) + "**********"
	samfile.close()
	return (kmer_list)

def findPosition(file):
        samfile = pysam.AlignmentFile(PATH+file, "rb")
        heat_list = []
        for r in samfile.fetch(until_eof=True):
                if r.is_unmapped:
                        position = r.next_reference_start
                        round_pos = int(round(position,-3)) 
			heat_list.append(round_pos) 

        print "******** WORKING ON: " + file + "  WITH LENGTH:" + str(len(heat_list)) + "**********"
        samfile.close()
        #findPosition(heat_list)
        return (heat_list)


def filterKmer(tuple):
	if tuple[1] >= 10 and tuple[1] <= 200:
		return tuple
	else:
		return None

def bamFiles():
        bamUrl = os.listdir("/home/ubuntu/genome_project/spark/bam_files")
        #distFiles = sc.parallelize(bamUrl)
#        if not os.path.exists(KUL + "/heat/"):
#                os.makedirs(KUL + "/heat/")
#        if not os.path.exists(KUL + "/kmer/"):
#                os.makedirs(KUL + "/kmer/")
	bamFiles = bamUrl[:1]
        distFiles = sc.parallelize(bamFiles)
	
	if str(sys.argv[1]) == "kmers":	
	
		kmer_res = distFiles.flatMap(lambda file: findKmers(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		#kmer_res.saveAsTextFile("output_RAW")
		kmer_range = kmer_res.map(lambda line: filterKmer(line)).filter(lambda obj: obj != None)
        	kmer_range.saveAsTextFile("kmers_range")
        	for obj in kmer_range.collect():
                	with open("/home/ubuntu/genome_project/spark/EVERYTHING.txt", "a") as f:
                        	f.write(str(obj) + "\n")

	elif str(sys.argv[1]) == "heat":  
	
		heat_res = distFiles.flatMap(lambda file: findPosition(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		heat_res.saveAsTextFile("heat_range")
		for obj in heat_res.collect():
               		with open("/home/ubuntu/genome_project/spark/POSITIONS.txt", "a") as f:
                        	f.write(str(obj) + "\n")
	
    
bamFiles()
