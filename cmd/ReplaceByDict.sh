/opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
--class ReplaceByDict \
--master yarn \
--deploy-mode cluster \
--queue root.default \
--driver-memory 7g \
--executor-memory 7G \
--num-executors 300 \
GraphxNet.jar \

