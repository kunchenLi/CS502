rmf output

register target/pig-udf-0.0.1-SNAPSHOT.jar
--register /home/hadoop/hadoop-2.9.0/share/hadoop/tools/lib/hadoop-aws-2.9.0.jar
--register /home/hadoop/hadoop-2.9.0/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.199.jar
register ../resource/geoip-api-1.3.1.jar
register ../resource/nutch-1.12.jar

a1 = LOAD 'access_log_sample' AS (line:chararray);
a2 = filter a1 by line matches '.*&url=(https:|http:|https%3A|http%3A)//play.google.com/store/apps/details.*';
a3 = FOREACH a2 GENERATE flatten(REGEX_EXTRACT_ALL(line, '(.*?) .*?\\[(.*?)\\].*?&url=(.*?)(?:&|%26).*')) as (ip:chararray, dt:chararray, url:chararray);
a4 = FOREACH a3 GENERATE ip, flatten(REGEX_EXTRACT_ALL(url, '.*?id(?:=|%3D)(.*)?')) as (id:chararray);
a5 = FILTER a4 by ip is not null;
a6 = FOREACH a5 generate ip, com.example.pig.GetCountry(ip) as country, id;

a7 = DISTINCT a6;
a8 = FILTER a7 by country == 'US';

--b1 = load 's3a://daijytest/nutchdb/segments/*/parse_data/part-*/data' using com.example.pig.NutchParsedDataLoader();
b1 = load 'appmetadata.txt.gz' as (url: chararray,title: chararray,name: chararray,publisher: chararray,updateTime: chararray,category: chararray,price: chararray,reviewScore: chararray,reviewCount: chararray,install: chararray,version: chararray,rating: chararray,developerSite: chararray,developerEmail: chararray);
b2 = filter b1 by $0 is not null;
b3 = foreach b2 generate flatten(REGEX_EXTRACT_ALL(url, '.*?id=(.*)?')) as (id:chararray), category;

c = join a8 by id, b3 by id;
d = group c by category;
e = foreach d generate group, COUNT(c) as count;
f = order e by count desc;

dump f;
