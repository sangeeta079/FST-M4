-Load data from HDFS
inputAllDialogues = LOAD 'hdfs:///user/root/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-Filter out the first 2 lines
ranked = RANK inputAllDialogues;
OnlyDialogues = FILTER ranked BY (rank_inputAllDialogues > 2);

-Group ny name
groupByName = GROUP OnlyDialogues BY name;

-Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

-Store result in HDFS
STORE namesOrdered INTO 'hdfs:///user/root/outputs/episodeOutput' USING PigStorage('\t');

-Store ouput in local system
hdfs dfs -copyToLocal 'hdfs:///user/root/outputs/episodeOutput' 'C:/Users/093528744/Desktop/M4/Final'  