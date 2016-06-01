from pyspark import SparkContext, SparkConf
import pysam,os, sys
conf = SparkConf().setAppName("appname").setMaster("spark://kgproject-4.openstacklocal:7077")
sc = SparkContext(conf=conf)

PATH = "/home/ubuntu/genome_project/spark/bam_files/"
KMER_PATH = "/home/ubuntu/genome_project/spark/txt_files/"
HEAT = 	    "/home/ubuntu/genome_project/spark/heat/"
KUL =       "/home/ubuntu/genome_project/spark"
KMER =      "/home/ubuntu/genome_project/spark/kmer/"
BAM_PATH = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"
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
       	try: 
		samfile = pysam.AlignmentFile(BAM_PATH+file, "rb")
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
	except: 
		if os.path.exists("/home/ubuntu/genome_project/spark/"+file+".bai"):
			os.remove("/home/ubuntu/genome_project/spark/"+file+".bai")
		return (kmer_list)

	
	else: 
		if os.path.exists("/home/ubuntu/genome_project/spark/"+file+".bai"):
                        os.remove("/home/ubuntu/genome_project/spark/"+file+".bai")
		return (kmer_list)

def findPosition(file):
        try:
		samfile = pysam.AlignmentFile(BAM_PATH+file, "rb")
        	heat_list = []
        	for r in samfile.fetch(until_eof=True):
                	if r.is_unmapped:
                        	position = r.next_reference_start
                        	round_pos = int(round(position,-3)) 
				heat_list.append(round_pos) 

        	print "******** WORKING ON: " + file + "  WITH LENGTH:" + str(len(heat_list)) + "**********"
        	samfile.close()
        except:
                if os.path.exists("/home/ubuntu/genome_project/spark/"+file+".bai"):
                        os.remove("/home/ubuntu/genome_project/spark/"+file+".bai")
                return (heat_list)

        else:
                if os.path.exists("/home/ubuntu/genome_project/spark/"+file+".bai"):
                        os.remove("/home/ubuntu/genome_project/spark/"+file+".bai")
                return (heat_list)


def extractKmers(tuple):
	if tuple[1] >= 10 and tuple[1] <= 200:
		return tuple
	else:
		return None

def bamFiles():
	all_files = sc.textFile("/home/ubuntu/genome_project/spark/all_files/index.html")
	bamFiles = []
	for file in all_files.collect():
		if file[-3:] == "bam":
			bamFiles.append(file) 
	print bamFiles 
       
	run_bamFiles = bamFiles[:2]
        distFiles = sc.parallelize(run_bamFiles)
	
	if str(sys.argv[1]) == "kmers":	
	
		kmer_res = distFiles.flatMap(lambda file: findKmers(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		#kmer_res.saveAsTextFile("output_RAW")
		kmer_range = kmer_res.map(lambda line: extractKmers(line)).filter(lambda obj: obj != None)
        	#kmer_range.saveAsTextFile("/home/ubuntu/genome_project/spark/kmers_range")
        	for obj in kmer_range.collect():
                	with open("/home/ubuntu/genome_project/spark/EVERYTHING.txt", "a") as f:
                        	f.write(str(obj) + "\n")

	elif str(sys.argv[1]) == "heat":  
	
		heat_res = distFiles.flatMap(lambda file: findPosition(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		#heat_res.saveAsTextFile("heat_range")
		for obj in heat_res.collect():
               		with open("/home/ubuntu/genome_project/spark/POSITIONS.txt", "a") as f:
                        	f.write(str(obj) + "\n")
	
    
bamFiles()
