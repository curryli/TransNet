hadoop fs -rm -r /user/hdrisk/AML/input_card
hadoop fs -rm -r /user/hdrisk/AML/MD5_card
hadoop fs -rm -r /user/hdrisk/AML/output_Score
hadoop fs -mkdir /user/hdrisk/AML/input_card
hadoop fs -put /home/hdrisk/input_card/card_test.csv /user/hdrisk/AML/input_card
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
--driver-memory 5g \
--executor-memory 5G \
--num-executors 100 \
/home/hdrisk/jar/Score_Spark.jar $1 $2

hadoop fs -getmerge /user/hdrisk/AML/output_Score /home/hdrisk/output_Score/output_Score.csv
echo "All Shell Done."
