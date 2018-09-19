#!/usr/bin/env bash

cd ~/IdeaProjects/git/accumulo-examples
mvn clean package
\rm -rf ~/Downloads/_tmp/ftpFile/accumulo-examples-2.0.0-SNAPSHOT.jar
cp -r /Users/hello/IdeaProjects/git/accumulo-examples/target/accumulo-examples-2.0.0-SNAPSHOT.jar  ~/Downloads/_tmp/ftpFile
ftpupload

cd ~/IdeaProjects/git/accumulo-examples


cd ~/IdeaProjects/git/hbase-operations-with-java
mvn clean package
\rm -rf ~/Downloads/_tmp/ftpFile/original-hcg-1.0-SNAPSHOT.jar
cp -r ~/IdeaProjects/git/hbase-operations-with-java/target/original-hcg-1.0-SNAPSHOT.jar  ~/Downloads/_tmp/ftpFile
ftpupload


#cd ~/IdeaProjects/git/hbase-operations-with-java

#\rm -rf original-hcg-1.0-SNAPSHOT.jar
#ftpget original-hcg-1.0-SNAPSHOT.jar
#scp -r original-hcg-1.0-SNAPSHOT.jar   root@web1.bj5:/home/work/wx/hbase-operations-with-java/target
#
#\rm -rf accumulo-examples-2.0.0-SNAPSHOT.jar
#ftpget accumulo-examples-2.0.0-SNAPSHOT.jar
#scp -r accumulo-examples-2.0.0-SNAPSHOT.jar  root@web1.bj5:/home/work/wx/accumulo-examples/lib/accumulo-examples-2.0.0-SNAPSHOT.jar

