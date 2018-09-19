
########################################################################################################################

export PATH=$PATH:/home/work/wx/accumulo/bin
accumulo shell -u root 


deletetable batch
createtable batch

config -t batch
config -t batch -s table.walog.enabled=false

addsplits -t batch g n t





cd /home/work/wx/accumulo-examples/
MY_CLASSPATH=/home/work/wx/accumulo-examples/lib/*:/home/work/accumulo-examples/lib/accumulo-examples-2.0.0-SNAPSHOT.jar
MY_CLASSPATH=$MY_CLASSPATH:`hadoop classpath`:`hbase classpath`
export PATH=$PATH:/home/work/wx/accumulo/bin
#java -Xmx10240m -Xms10240m  -cp $MY_CLASSPATH  org.apache.accumulo.examples.client.MultiThreadedAccClientExample  $1 $2 $3 $4 $5 $6



accumulo shell -u root -p yidu -e "scan -t batch -np" |wc -l

accumulo shell -u root -p yidu -e "scan -t batch"

accumulo shell -u root -p yidu -e "config -t batch" 



echo yes| accumulo shell -u root -p yidu -e "deletetable batch"
accumulo shell -u root -p yidu -e "createtable batch" 
accumulo shell -u root -p yidu -e "addsplits -t batch 1 2 3 4 5 6 7 8 9 0 a e j m o t u z" 

accumulo shell -u root -p yidu -e "config -t batch -s table.walog.enabled=false"




nohup  sh acc_run_wx.sh  16  1024  1 2 5 1  > acc_times.txt &

# 参数说明: 线程池大小, value 大小, 线程数, 每个线程处理数据条数,列簇数量,列数量, 随机key因子

cat acc_times.txt |grep -E "GENERATEDATA_TIME" | awk -F '\t' '{sum += $2} END {print sum}'
cat acc_times.txt |grep -E "INSERT_TIME" | awk -F '\t' '{sum += $2} END {print sum}'
cat acc_times.txt |grep -E "ALL_TIME" | awk -F '\t' '{sum += $2} END {print sum}'
cat acc_times.txt |grep -E "INSERT_ROWS" | awk -F '\t' '{sum += $2} END {print sum}'


sh acc_run_wx.sh  16  1024  20 10000 1 1 3

java -Xmx10240m -Xms10240m  -cp $MY_CLASSPATH  org.apache.accumulo.examples.client.AccRandomBatchScannerMultiThreaded 16 20




## 20w



sh acc_run_wx.sh  16  1024  20 10000 5 1 3


nohup  sh acc_run_wx.sh  16  10240  25 8000 5 1 3 > acc_times.txt &


nohup  sh acc_run_wx.sh  16  102400  250 800 5 1  > acc_times.txt &



## 50w

nohup  sh acc_run_wx.sh  16  1024  50 10000 5 1  > acc_times.txt &



nohup  sh acc_run_wx.sh  16  10240  50 10000 3 1  > acc_times.txt &


nohup  sh acc_run_wx.sh  16  102400  500 1000 5 1  > acc_times.txt &






## 100w

nohup   sh acc_run_wx.sh 16  1024  100 10000 5 1  > acc_times.txt &

nohup   sh acc_run_wx.sh 16  10240  100 10000 1 1  > acc_times.txt &

nohup   sh acc_run_wx.sh 16  102400  1250 800  5 1  > acc_times.txt &




## 100w
nohup   sh acc_run_wx.sh 10  102400  1250 800  10 1  3  > acc_times_100w_100_10_1.txt &



nohup   sh acc_run_wx.sh 10  1048576  4000 50  10 1  3  > acc_times_20w_1024_10_1.txt &


9*100 * 10 * 1024 / 1024./1024

10 * 1048576 * 200000 / 1024./1024/1024




## 100w
nohup   sh acc_run_wx.sh 10  102400  1250 800  10 1  3  > acc_times_100w_100_10_1.txt &





## 1w
nohup   sh acc_run_wx.sh 10  512000  200 50  20 1  3  > acc_times_1w_500_20_1.txt &


nohup   sh acc_run_wx.sh 10  512000  100 100  10 1  3  > acc_times_1w_500_10_1.txt &



nohup   sh acc_run_wx.sh 10  1048576  100 50  10 1  3  > acc_times_1w_1024_10_1.txt &



50-20
nohup   sh acc_run_wx.sh 4  1048576  100 50  20 1  3  > acc_times_1w_1024_10_1.txt &


nohup   sh acc_run_wx.sh 20  1024  1000 5000  50 1  3  > acc_times_500w_1024_50_1.txt &














