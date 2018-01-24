infile=$1
startdate=$2
enddate=$3
outfile=$4

rm -rf /home/hdrisk/output_Score/output_Score.csv
hadoop fs -rm -r /user/hdrisk/AML/input_card
hadoop fs -rm -r /user/hdrisk/AML/MD5_card
hadoop fs -rm -r /user/hdrisk/AML/output_Score
hadoop fs -mkdir /user/hdrisk/AML/input_card
hadoop fs -put /home/hdrisk/input_card/$1 /user/hdrisk/AML/input_card
hadoop fs -ls /user/hdrisk/AML/input_card
hadoop jar  /home/hdrisk/jar/CardMd5.jar com.unionpay.data.mapreduce.EncryptCardMR /user/hdrisk/AML/input_card /user/hdrisk/AML/MD5_card
echo "hadoop fs -ls /user/hdrisk/AML/MD5_card:"
hadoop fs -ls /user/hdrisk/AML/MD5_card

echo "MD5 Done."
echo "Start Spark Score..."


spark-submit \
--class Service.ScoreMd5Cards \
--master yarn \
--deploy-mode cluster \
--queue root.queue_hdrisk \
--driver-memory 15g \
--executor-memory 15G \
--num-executors 200 \
/home/hdrisk/jar/Score_Spark.jar $2 $3

hadoop fs -getmerge /user/hdrisk/AML/output_Score /home/hdrisk/output_Score/$4
echo "All Shell Done."
