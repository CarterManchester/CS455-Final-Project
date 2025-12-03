# Investigating Correlations Between Social Vulnerability and Prison Populations

## Links:

Dataset Loading: https://urban-sustain.org/data-download

SVI Index: https://svi.cdc.gov/map25/data/docs/SVI2022Documentation_ZCTA.pdf <br><br><br><br><br>



## Notes for Starting HDFS and Spark:
- Make sure you are SSH-ed into your namenode.
- To run Hadoop in LOCAL MODE, ssh into your secondarynamenode first.
    - Then make sure the line ` .master(`local[*]`) ` in Main.java is not commented out.
    - To run in local mode, use command: `gradelew run`. (cd ../local_directory/CS455-Final-Project)

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

- Run (...uhhh TBD... see local run above...): 
    

- Save as Tar (cd cs455-Final-Project/app):
     tar -cvf LASTNAME-FIRSTNAME.tar src/ build.gradle <br><br><br><br><br>



## Important Variables Used in This Project:
#### (NOTE: The following information was found directly from the SVI Index Documentation link at the top of this README. Please see the documentation for more information) <br><br>

- #### Important Prefixes to Variables:
    - `E` Variables: `Obtain estimates of the CDC/ATSDR SVI variables from the Census Bureau.`
    - `EP` Variables: `Obtain or derive percentages for the 16 CDC SVI variables.` (Used the most in this project!)
    - `EPL` Variables: `Rank the `EP` variables to get percentile rankings (or the CDC/ATSDR SVI rankings) for each of the 16 variables.`
    - `SPL` Variables: `Sum the `EPL` variables by theme.`
    - `RPL` Variables: `Rank the theme-specific `SPL`variable.`
    - Overall `SPL` Variable (`SPL_THEMES`): `Sum the `SPL` variables from all four themes.`
    - Overall `RPL` Variable (`RPL_THEMES`): `Rank `SPL_THEMES`. This is the overall summary ranking variable.`<br><br>

- #### Locational Variables:
    - `STATE` - A State in the US.
    - `COUNTY` - A County in a State.
    - `FIPS` - Geographic identification (state, county, and sometimes census tract)<br><br>

- #### Overall Vulnerability Variables:
    - `RPL_Themes` - Overall summary variable, ranking the below 4 themes as a percentage.
    - `RPL_Theme1` - Socioneconomic Status (VERY IMPORTANT TO THIS PROJECT!)
    - `RPL_Theme2` - Household Characteristics
    - `RPL_Theme3` - Racial and Ethnic Minority Status
    - `RPL_Theme4` - Housing Type and Transportation<br><br>

- #### `RPL_Theme1` Variables:
    - `EP_POV` - Percentage of people below the 150% poverty estimate.
    - `EP_UNEMP` - Percentage of people above the age of 16 unemployed.
    - `EP_PCI` - Per capita income (as % rank).
    - `EP_NOHSDP` - Percentage of people above the age of 25 with no high school diploma.
    - `EP_UNINSUR` - Percentage of people without health insurance.
 
- #### `RPL_Theme2` Variables:
    - `EP_DISABL` - Percentage of people with disabilities (not institutionalized).
    - `EP_SNGPNT`,- Percentage of single parent households with children under 18.
    - `EP_LIMENG` - Percentage of people who know only limited English.

- #### `RPL_Theme3` Variables:
    - `EP_MINRTY` - Percentage Minority estimate.
 
- #### `RPL_Theme4` Variables:
    - `EP_MUNIT` - Percentage of housing in structures with 10+ units (like apartments)
    - `EP_MOBILE` - Percentage of housing that are mobile homes
    - `EP_CROWD` - Percentage of households with more than 1 person per room (crowding)
    - `EP_NOVEH` - Percentage of households with no available vehicle
    - `EP_GROUPQ` - Percentage of Persons in Group Quarters: includes prisons, jails, college dorms, group homes, etc. and can show us which counties have large prison facilities and the number of prisons per county.
