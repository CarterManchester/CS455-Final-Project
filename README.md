# CS455-Final-Project

## Links:

Dataset Loading: https://urban-sustain.org/data-download

SVI Index: https://svi.cdc.gov/map25/data/docs/SVI2022Documentation_ZCTA.pdf



## Notes for Starting HDFS and Spark:
- Make sure you are SSH-ed into your namenode.
- To run Hadoop in local mode, ssh into your secondarynamenode first.
    - then..... not sure yet...

- `jps` - Check to see if HDFS namenode and cluster are running.
- `cat ~/hadoop_spark_configs/README.md` - to see all of your machines and their roles.
- `~/hadoopConf/monitor.sh` - to check all machines states.
- ### Run HDFS Commands (remember to ssh into namenode first):
    - `start-dfs.sh`
    - `stop-dfs.sh` (doesnt affect data stored in HDFS)
- ### Run Spark Commands:
    - `start-master.sh` and `start-workers` to start Spark cluster.
    - `stop-workers.sh` and `stop-master.sh` to turn off Spark cluster.

## Notes to Run Program:

- Compile (cd cs455-Final-Project/app):
    gradle build

- Run (...uhhh TBD...): 
    hadoop jar <your_jar> <state1> <state2> <state3> … <input_folder> <output_folder>  (this was for hw4. not this assignment.)

- Save as Tar (cd cs455-Final-Project/app):
     tar -cvf LASTNAME-FIRSTNAME.tar src/ build.gradle 
