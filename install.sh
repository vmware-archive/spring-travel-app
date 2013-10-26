#!/bin/sh

export GEMFIREXD=/export/gcm/where/gemfireXD/10/snaps.Any/snapshots.44310/
export GEMFIREXD_JAR=$GEMFIREXD/product-sqlf/lib/sqlfire.jar
export GEMFIREXD_CLIENT_JAR=$GEMFIREXD/product-sqlf/lib/sqlfireclient.jar
export POSTGRES_JAR=/kuwait1/users/bansods/postgresql-9.2-1003.jdbc4.jar
export PATH=$PATH:$GEMFIREXD/bin
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$GEMFIREXD_JAR:$GEMFIREXD_CLIENT_JAR

mvn install:install-file -DgroupId=com.gopivotal.sqlfire -DartifactId=sqlf-dialect -Dversion=1.0 -Dpackaging=jar -Dfile=$GEMFIREXD/hidden/lib/sqlfHibernateDialect.jar
mvn install:install-file -DgroupId=com.gopivotal.sqlfire -DartifactId=sqlf-client -Dversion=1.0 -Dpackaging=jar -Dfile=$GEMFIREXD_CLIENT_JAR
mvn install:install-file -DgroupId=com.gopivotal.sqlfire -DartifactId=sqlfire -Dversion=1.0 -Dpackaging=jar -Dfile=$GEMFIREXD_JAR
mvn install:install-file -DgroupId=com.gopivotal.hawq.jdbc -DartifactId=hawq-jdbc4-driver -Dversion=9.2-1003.jdbc4 -Dpackaging=jar -Dfile=$POSTGRES_JAR

echo "Building the spring-travel-app....."
cd booking-mvc && mvn clean package
cd ..

echo "Building the map-reduce application to update the hotel prices based on the bookings"
cd sta-mapreduce && mvn clean package
cd ..

echo "Building the jdbc application to read bookings data from the HDFS using HAWQ and updating the hotel prices in GemfireXD"
cd sta-hawq-gfxd-hotelpriceupdater && mvn clean compile assembly:single
cd ..








