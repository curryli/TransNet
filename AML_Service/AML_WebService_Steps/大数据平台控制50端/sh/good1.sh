hadoop fs -rm -r /user/hdrisk/AML/input_card
hadoop fs -rm -r /user/hdrisk/AML/MD5_card
hadoop fs -rm -r /user/hdrisk/AML/output_card
hadoop fs -mkdir /user/hdrisk/AML/input_card
hadoop fs -put /home/hdrisk/input_card/card_test.csv /user/hdrisk/AML/input_card
hadoop fs -ls /user/hdrisk/AML/input_card
hadoop jar  /home/hdrisk/jar/CardMd5.jar com.unionpay.data.mapreduce.EncryptCardMR /user/hdrisk/AML/input_card /user/hdrisk/AML/MD5_card
echo "hadoop fs -ls /user/hdrisk/AML/MD5_card:"
hadoop fs -ls /user/hdrisk/AML/MD5_card
