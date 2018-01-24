spark-submit \
--class Service.ScoreReadCards \
--master yarn \
--deploy-mode cluster \
--queue root.default \
--driver-memory 15g \
--executor-memory 15G \
--num-executors 300 \
TeleTrans.jar "20170511" "20170511" "xrli/AML/Inputs/seedCards_test.csv" "xrli/AML/Outputs/CardsScore_test.csv"
