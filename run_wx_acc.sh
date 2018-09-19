

if [ $# -ne 6 ]; then
    echo "aaaaaaaaaaaa"
    exit 1
fi


cd /home/work/wx/accumulo-examples/
MY_CLASSPATH=/home/work/wx/accumulo-examples/lib/*:/home/work/accumulo-examples/lib/accumulo-examples-2.0.0-SNAPSHOT.jar
MY_CLASSPATH=$MY_CLASSPATH:`hadoop classpath`:`hbase classpath`
export PATH=$PATH:/home/work/wx/accumulo/bin

date
java -Xmx10240m -Xms10240m  -cp $MY_CLASSPATH  org.apache.accumulo.examples.client.MultiThreadedAccClientExample  $1 $2 $3 $4 $5 $6
date

