#!/bin/bash

#Assume that there are 3 Hadoop jobs to compute PageRank: step1.jar, step2.jar, step3.jar

N=10 #termination condition

myflow_id=$1

input_s3=$2

output_s3=$3


#Use AWS CLI to copy jar files to input_s3 (S3)

aws s3 cp Step1.jar $input_s3/

aws s3 cp Step2.jar $input_s3/

aws s3 cp Step3.jar $input_s3/


#RUN THE FIRST JOB

./elastic-mapreduce -j $myflow_id --jar ${input_s3}/Step1.jar --arg -input --arg s3://cs9223/pagelinks-en-all.csv --arg -output --arg ${output_s3}/Job0/

./elastic-mapreduce -j $myflow_id --wait-for-steps

#Copy output of the first job to $output_s3/loop0


#aws s3 cp ${output_s3}/loop0/part-r-00000 $output_s3/Job0/


#RUN THE SECOND JOB

for i in $(seq 1 $N)
do
      ((k=i-1))
  echo ${output_s3}/Job${k}/part-r-00000
  echo ${output_s3}/Job${i}/
      
	./elastic-mapreduce -j $myflow_id --jar ${input_s3}/Step2.jar --arg -input --arg  ${output_s3}/Job${k}/part-r-00000 --arg -output --arg ${output_s3}/Job${i}/ 

	./elastic-mapreduce -j $myflow_id --wait-for-steps

done

#RUN THE THIRD JOB

echo ${output_s3}/Job${N}/part-r-00000

./elastic-mapreduce -j $myflow_id --jar ${input_s3}/Step3.jar --arg -input --arg ${output_s3}/Job${N}/part-r-00000 --arg -output --arg ${output_s3}/xyz/

./elastic-mapreduce -j $myflow_id --wait-for-steps



aws s3 cp ${output_s3}/xyz/part-r-00000 ${output_s3}/top.txt



