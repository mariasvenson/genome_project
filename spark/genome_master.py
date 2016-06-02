from pyspark import SparkContext, SparkConf
import pysam,os, sys
import numpy as np

conf = SparkConf().setAppName("genome_project").setMaster("spark://kgproject-4.openstacklocal:7077")
sc = SparkContext(conf=conf)

PATH = "/home/ubuntu/genome_project/spark/bam_files/"
KMER_PATH = "/home/ubuntu/genome_project/spark/txt_files/"
KMER =      "/home/ubuntu/genome_project/spark/kmer/"
BAM_PATH = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"

# graph
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.tools as pl
pl.set_credentials_file(username='mariasvenson', api_key='rgphv7aivd')

#Read BAM file, find unmapped reads and extract all kmers 
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

#Read BAM file, find unmapped reads and the position of the mate/next read.
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


#Extract all kmers within tange 10-200
def extractKmers(tuple):
	if tuple[1] >= 10 and tuple[1] <= 200:
		return tuple
	else:
		return None


#Create a list of all BAMfiles
def bamFiles():
	all_files = sc.textFile("/home/ubuntu/genome_project/spark/all_files/index.html")
	bamFiles = []
	for file in all_files.collect():
		if file[-3:] == "bam":
			bamFiles.append(file) 
	print bamFiles 
       
	run_bamFiles = bamFiles[:2]
	
#Parallelizing the BAMfile names
        distFiles = sc.parallelize(run_bamFiles)
	
	if str(sys.argv[1]) == "kmers":	

		kmer_res = distFiles.flatMap(lambda file: findKmers(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		#kmer_res.saveAsTextFile("output_RAW")
		kmer_range = kmer_res.map(lambda line: extractKmers(line)).filter(lambda obj: obj != None)
        	#kmer_range.saveAsTextFile("/home/ubuntu/genome_project/spark/kmers_range")
        	for obj in kmer_range.collect():
                	with open("/home/ubuntu/genome_project/spark/result_kmers.txt", "a") as f:
                        	f.write(str(obj) + "\n")

	elif str(sys.argv[1]) == "pos":  
	
		heat_res = distFiles.flatMap(lambda file: findPosition(file)).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
		#heat_res.saveAsTextFile("heat_range")
		for obj in heat_res.collect():
               		with open("/home/ubuntu/genome_project/spark/result_positions.txt", "a") as f:
                        	f.write(str(obj) + "\n")
	


#N = 500
	plot_x = []
	plot_y = []

	for obj in heat_res.collect():
		plot_x.append(obj[0])
		plot_y.append(obj[1])

	trace0 = go.Scatter(
    	x = plot_x,
    	y = plot_y,
    	name = 'unmapped',
    	mode = 'markers',
    	marker = dict(
        	size = 8,
        	color = 'rgba(152, 0, 0, .8)',
        	line = dict(
            	width = 2,
            	color = 'rgb(0, 0, 0)'
	        )
	    )
	)

	data = [trace0]

	layout = dict(title = 'Styled Scatter',
        	      yaxis = dict(zeroline = False),
         	      xaxis = dict(zeroline = False)
	             )

	fig = dict(data=data, layout=layout)
	py.iplot(fig, filename='unmapped-scatter')

bamFiles()
