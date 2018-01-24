spark-submit \
--class Service.ScoreMd5Cards \
--master yarn \
--deploy-mode cluster \
--queue root.queue_hdrisk \
--driver-memory 15g \
--executor-memory 15G \
--num-executors 200 \
/home/hdrisk/jar/Score_Spark.jar "20170620" "20170620"
