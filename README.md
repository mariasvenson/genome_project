# Genome sequencing project 

### Description: 
The purpose of this study is to in an computationally effective manner study structural variation of chromosome 20 in over 2500 human genomes from the 1000 genomes project low-coverage data.

### How to run the program: 
To find either positions or kmers of the unmapped reads in the 1000 genome dataset (http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/), follow the guide below:

1. find the file "genome_master" in genome_project/spark/genome_master.py

By entering a flag as the second argument when running the file, the program either 
extracts all the positions or kmers depending of your choise. 

Finding the kmers, enter the [FLAG] = "kmers"
Finding the position of the unmapped reads mate, enter the [FLAG] = "pos"

2. Run the program: 
```python python genome_project [FLAG]
```






