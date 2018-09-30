# BigDataProcessing_Assignment
Assignment for Big Data Processing - Map Reduce centered 

### How to work with the program on AWS
1. Login to jump host (e.g. ssh ec2-user@<student_id>.jump.cosc2637.route53.aws.rmit.edu.au -i <path_to_key>/<student_id>-cosc2637.pem)
2. Initiate cluster (create.sh)
3. Add jump host to known host for ssh (sed -i '7d' ~/.ssh/known_hosts)
    Alternatively set back directly in known_host vi /home/ec2-user/.ssh/known_hosts   
4. Ssh into hadoop - ssh hadoop@<student_id>.emr.cosc2637.route53.aws.rmit.edu.au -i <student_id>-cosc2637.pem
    - make sure the key on jumphost
5. Open hue (Website: http://<student_id>.hue.cosc2637.route53.aws.rmit.edu.au:8888)
6. Get dataset - e.g. wet files - go to server file - S3 - add “commoncrawl” and get a suitable dataset and copy the path 
7. Copy data set to tmp on cluster
hadoop distcp s3a://commoncrawl/crawl-data/CC-MAIN-2018-39/segments/1537267155413.17/wet/CC-MAIN-20180918130631-20180918150631-00000.warc.wet.gz /tmp
8. Upload jar file to hadoop via hue (which means it is on the cluster but has to be still transfered to the master node)
9. Move jar file to master node - hadoop fs -copyToLocal <path_to_jar>/counter-1.0.1-SNAPSHOT.jar /home/hadoop/ 
10. Executive the job from master - hadoop jar /home/hadoop/counter-1.0.1-SNAPSHOT.jar pjdk.hadoop.cooccurrence.CoOccurrence <path to dataset> <number of mappers> <Execution_Option> 

### Options for Execution
occurrence = Plain Pairs Implementation
occurrencecombiner = Pairs Implementation with optional Combiner
occurrencepartitioner = Pairs Implementation with optional Partitioner
occurrenceinmaplocal = Pairs Implementation with optional in-mapper combiner (for local aggregation)
occurrenceinmapglobal = Pairs Implementation with optional in-mapper combiner (for local aggregation)

stripes = Plain Stripes Implementation
stripescombiner = Stripes Implementation with optional Combiner

### How to compile the Program 
- Make sure Maven is installed on your environment - (brew install maven). 
- Open Project or go to project root folder where the 'pom' file resides
- run command: mvn clean package
- upload counter-1.0.1-SNAPSHOT.jar (usually to find in BigDataProcessing_Assignment/target) to hadoop master node

### Additional Sources
[Trelloboard of the project](https://trello.com/b/YUgGpREg)

[Test Matrix for Analysis](https://docs.google.com/spreadsheets/d/15L-Alyr0IGbKzOgYffi7iBOomBxnTuQdqfJTmyYDDlE/edit?usp=sharing)

### Aim
to design, implement, critically analyse and report on a substantial big data project.
Due:due 11:59 pm on Tuesday 9 October 2018
Marks:This assignment is worth 30% of your overall mark

### Requirements
Using a big public AWS dataset write a MapReduce program undertake an interesting new analysis of the data. 
Compare the performance of your program using different size clusters.This assignment can be either as a individual or
 in groups of two students.  If you form a team, make sure to include the name of your partner in the submission’s 
 description onCanvas, filenames that are named after your student numbers, the report and the presentation.

### Deliverables
- Code, output and log files
- Written report
- Presentation

### Code, output and logs (15 marks)
- You must use AWS EMR (such as provided by RMIT through this course) for running your MapReduce program 
(which may be developed on any machine) and access in gone (or more) of the public data sets in S3, 
such as:
1.s3://datasets.elasticmapreduce/ngrams/books/ 
2.s3://aws-publicdatasets/common-crawl/ 
You do NOT have to use these two datasets. 
The full list of datasets can be found here:https://registry.opendata.aws/
- In choosing a dataset(s) and analysis to undertake, a combination of the two factors may contribute to novelty. 
In particular, for a similar difficulty of analysis task, more novelty will be given for using the common-crawl instead of 
ngrams, and even more novelty will be given for another similar size dataset available ons3.•Paths should not be 
hard-coded.•As you are required to time your tasks, you should carefully log your experiments on different sized clusters.

- Your MapReduce program(s) must be well written, using good coding style and in-cluding appropriate use of comments. 
 Your markers will look at your source code. Coding style will form a small part of the assessment of this assignment. 
- You should provide instructions with your code on how to compile and execute it.If your marker cannot compile your
programs, you risk yielding zero marks for the coding component of your assignment.•Depending  on  the  size  of  
output  your  MapReduce  program  generates,  you  may choose or need to submit only samples of the output 
(in which case you should also provide a complete summary of all the output).
- Depending on your implementation, you may wish to provide additional information about your code 
(for example, how it is to be compiled and run), output and logs. If so, put this information into a plain text file called 
readme.txt.•You can use either Java, Python, Hive or Pig to develop your MapReduce programover AWS EMR.

### Results of experiments and written report (10 marks)
- You are required to write a short report in the style of a conference paper on your experiments.
- The text of the report should be only TWO A4 pages (not including references and appendices), using the IEEE conference 
template for Word or LaTex http://www.ieee.org/conferences_events/conferences/publishing/templates.htmlhowever you may 
provide additional material including references, and appendices containing output, tables, graphs in additional pages. 
Your report must be a PDF file, called sNNNNNNN(sNNNNNNN).report.pdf, wheresNNNNNNN is your student number and the optional 
partner’s student number.  Files that do not meet this requirement may not be marked. Your report must be well-written.
- Your report should include:
    1. Explain how you implemented your map reduce program.
    2. Undertake experiments using your programs and report on the output and timings.
    3. Discuss your results and critically analyse the scalability of your approach as more nodes are added to the cluster. 
    Are the results as you expected? 
    4. Compare what you have done with published studies using the same data.
Important:Your report will be marked on the quality of your written explanations and analysis. 
After writing your report you should carefully revise it checking for clarity of expression and quality of writing.
 
### Presentation (5 marks)
- Your presentation must be a one page PDF file, called sNNNNNNN.presentation.pdf, 
where sNNNNNNN is your student number. Files that do not meet this requirement may not be marked. 
- To be conducted during tute/labs in week 12, ONLY students who have submitted their presentations 
(assNNNNNNN.presentation.pdf) when due 11:59 pm on Tues-day 9 October 2018 will be eligible to present in class.  
Students must attend theirscheduled tute/lab and be ready to present at their allocated time.

### FAQ
Q1. If I do not have any idea to choose which algorithm to work on my assignment, what should I do?
 
A1: You can choose one of below four topics to implement your assignment. You can also find exemplar standard based on the code rubrics how we mark your code.
 
1. Algorithm from Week 5 lecture: Implement the co-occurrence problem over common crawl dataset: 
https://registry.opendata.aws/commoncrawl/ , 
compare the performance of different optimisations such as pairs, stripes by changing number of clusters, 
dataset size, etc. If you implement this, you can get a fair score. If you further implement local aggregation, 
partitioner, combiner, you will get a good score.  If you further use multiple datasets from AWS except the common 
crawl to do above analysis, you will get full mark.

2. Algorithm from Week 6 lecture: k-means over NYC taxi pickup and drop-off points: 
https://registry.opendata.aws/nyc-tlc-trip-records-pds/ , 
compare the performance of different optimisations such as partitioner, combiner by cluster number and dataset size, etc. 
If you implement this, you can get a fair score. If you further implement the local aggregation and compare, you will 
get a good score. If you further visualise your result in a figure or a map by setting different k, or using multiple 
datasets from AWS, or show good ideas which can further improve the performance, you will get full mark. Moreover, the 
dataset on AWS may not contain the location information due to privacy issue, you can download the dataset we maintained 
before using your rmit email here: https://drive.google.com/open?id=1zFXNmWFA6z6kkCX0EfXPdbdD84aWbpFZ 

3. Algorithm from Week 8 lecture: Graph path search using OpenStreetMap data: https://registry.opendata.aws/osm/ 
compare the local aggregation, partitioner, combiner by changing the dataset size, number clusters, etc. 
Since we will not provide any tools for you to read the graph dataset and this lecture has not been delivered yet, 
if you can implement above requirements, you will get good score directly. If you further use multiple graph datasets 
from AWS, you will get full mark.
All above three topics can be done over the exemplar code we provide in our tut/labs, and the code is just basic 
implementation and we just use a toy dataset, you will get zero mark if you submit it without any change.
4. Your own algorithm: If you think all the three topics we provide are boring, you can find your own algorithms 
using MapReduce and compare the performance of local aggregation, partitioner, combiner and at least other two 
optimisations, then choose multiple datasets from AWS to test, you will get full mark.
**
For your report, based on the report rubric in Canvas, if you can list at least 2 line graphs or tables like the 
lecture 5, slide 20 to show the performance, you will get a fair score. If you can list at least 4 line graphs or tables, 
it will be easy to get a good score. If you list at least 8 line graphs or tables, it will be easy to get a full mark.
**
 
  
