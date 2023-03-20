-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/root/file01.txt' AS (lines);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group, COUNT(words);
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/root/results';