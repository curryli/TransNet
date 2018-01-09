spark-submit \
--class Service.ScoreMd5Cards \
--master yarn \
--deploy-mode cluster \
--queue root.queue_hdrisk \
--driver-memory 5g \
--executor-memory 5G \
--num-executors 100 \
/home/hdrisk/jar/ScoreCards.jar "20170501" "20170501"