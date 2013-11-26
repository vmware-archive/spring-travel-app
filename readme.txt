********************
a) Pre-requisites
********************
1) Pivotal HD setup
http://bitcast-a.v1.o1.sjc1.bitgravity.com/greenplum/pivotal-docs/P_HD_11_README.pdf

2) Starting the sqlfire locator and servers.
http://gemfirexd-05.run.pivotal.io/index.jsp?topic=/com.pivotal.gemfirexd.0.5-rn/pivotal-gemfirexd-0.5.beta.html

3) Setting up and starting HAWQ

**************************
b) Data Overview

**************************
GemfireXD Tables

1) booking - persisted to hdfs
2) hotel - in-memory
3) customer - in-memory
Please refer to sql_scripts/create_schema.sql for the definitions of the tables


************************************
c) Setting up and installing projects
************************************
This projects consists of 3 components 
1) booking-mvc - The spring mvc app which connects to GemfireXD
2) sta-mapreduce - The map reduce application which works on the booking table in the HDFS and updates the hotel prices in 'hotel' table(in-memory) in GemfireXD
3) sta-hawq-gfxd-hotelpriceupdater - The jdbc application which uses hawq to query the bookings data in HDFS and updates the prices of hotel in GemfireXD.

Edit the install.sh to modify the location of jars which are required by the above components
*Note that the sta-hawq-gfxd-hotelpriceupdater needs the lates postgres jdbc4 driver5 (postgresql-9.2-1003.jdbc4.jar).

Source the install.sh 
>source install.sh 

This would install all the maven dependencies and build all the components, and create "target" folders for each of the projects. 



******************************************************
d) Setting up the GemfireXD cluster and creating the schema.
******************************************************
1) Start the GemfireXD locators and servers

2) Create the schema required for this project and import the initial data required for the app.

The schema is defined in  sql_scripts/create_schema.sql, if you wish to use off_heap memory you must use 'sql_scripts/create_schema_offheap.sql'
Before running the script you will have to do the following modifications to the create_schema.sql
In the "create hdfsstore" statement change the value of the "namenode" parameter to point to the URL of the name node that you have started.

The URL format  : 'hdfs://<name_node_hostname>:<name_node_port>'

After the modifications. Start the sqlf command line tool and connect to the sqlfire locator

sqlf>connect client 'locator_hostname:port';
sqlf>run 'scripts/create_schema.sql';
sqlf>run 'scripts/import.sql';

******************************************************
e) Building and running the booking-mvc app.
******************************************************

1)Modify the booking-mvc app to connect to the gemfirexd locator.

2) In the app booking-mvc/src/main/webapp/WEB-INF/config/data-access-config.xml.

Modify the url property to point to the correct sqlfire locator of your cluster.

<bean id="dataSource" class="org.springframework.jdbc.datasource.DriverManagerDataSource">
        <property name="driverClassName" value="com.vmware.sqlfire.internal.jdbc.ClientDataSource" />
        <property name="url" value="jdbc:sqlfire://<locator_hostname>:<locator_port>/" />
        <property name="username" value="APP" />
        <property name="password" value="APP" />
</bean>

3) Build and run the app
>cd booking-mvc
>mvn tomcat:run

******************************************************
f) Creating Hotel Bookings using JMETER
******************************************************
1)Download JMeter from: http://jmeter.apache.org/

2) Open up the JMeter application.
With JMeter, open STATestPlan.jmx located in the checkout in the JMeter directory.
The test plan will need to be modified and can be modified to do different workloads.

-The first change that needs to be made is the location of the sqlfireclient.jar.

-Click on TestPlan (located in the left side menu)

-Click the Browse button to add directory or jar to classpath.

-Add the sqlfireclient.jar.

Next we need to edit the jdbc connection url
Expand the Test Plan and JDBC Users sub menus

Click on JDBC Connection Configuration

Modify the database url and replace the hostname and port with the location of your GemFireXD locator

Running the test plan:

Click on the Green Arrow located in the top menu bar or start the test under the Run menu.

Modifying the number of threads and iterations per thread

Click on JDBC Users.

Modify the number of threads and loop count fields to the desired amounts

Turning on the Debug Sampler/View Results Tree

Currently both are disabled.  You may enable them by right clicking on them and enabling.

Enabling will hinder performance but allow you to see the results of each step of the test plan.  It may be helpful in debugging issues with the test plan.


******************************************************
g) Building and running the map-reduce jar
******************************************************
1) In your checkout build the map-reduce jar , do the following

sta-mapreduce/target directory

Before running the mapreduce job , make sure you have the sqlfire.jar and sqlfireclient.jar on the HADOOP_CLASSPATH

>export HADOOP_CLASSPATH=/export/gcm/where/gemfireXD/10/snaps.Any/snapshots.43932/product-sqlf/lib/sqlfire.jar:/export/gcm/where/gemfireXD/10/snaps.Any/snapshots.43932/product-sqlf/lib/sqlfireclient.jar


Take that jar and run it on the machine where the hadoop is running and is part of your cluster;
Specify the "locator_host_name:locator_port" as the argument to the M-R job.


>yarn jar sta-mapreduce-1.0-SNAPSHOT.jar com.gopivotal.sta.mr.TopBusyHotel locator_hostname:locatorport


******************************************************
h) Querying the HDFS data via HAWQ
******************************************************
To query the data on HDFS , you would need to create an external table which maps to the GemfireXD table.
For mapping the table , you can choose the fields of your interest and map them.


create the database
bin/createdb mydb

Create the external table on HAWQ, 
you would have to enter the hostname and the http port of your name node in the pxf url.

bin/psql mydb

mydb=+> create external table Booking  (id bigint, hotel_id bigint)
location ('pxf://<name_node_hostname>:<name_node_http_port>/sta_tables/APP.BOOKING?Fragmenter=GemFireXDFragmenter&Accessor=GemFireXDAccessor&Resolver=GemFireXDResolver&CHECKPOINT=FALSE')
format 'custom' (formatter='pxfwritable_import');


To compile the jdbc aplication you would need the latest JDBC 4 driver for postgres
You can dowload it here http://jdbc.postgresql.org/download.html




