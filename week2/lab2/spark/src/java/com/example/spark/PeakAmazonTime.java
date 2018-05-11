package com.example.spark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.ParseData;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.ibm.icu.util.Calendar;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;


public class PeakAmazonTime {
static class Transformer implements Function<String, Row> {
        Pattern linePattern1 = Pattern.compile("(.*?) .*?\\[(.*?)\\].*?&url=(.*?)(?:&|%26).*");
        Pattern linePattern2 = Pattern.compile(".*?id(?:=|%3D)(.*)?");
        static LookupService cl;
        static Object lock = new Object();
        @Override
        public Row call(String line) throws Exception {
            Matcher m1 = linePattern1.matcher(line);
            String ip = null;
            String id = null;
            String dt = null;
            String url = null;
            if (m1.find()) {
                ip = m1.group(1);
                dt = m1.group(2);
                url = m1.group(3);
                Matcher m2 = linePattern2.matcher(url);
                if (m2.find()) {
                    id = m2.group(1);
                    dt = m1.group(2);
                }
            }
            synchronized(lock) {
                if (cl == null) {
                    cl = new LookupService(SparkFiles.get("GeoLiteCity.dat"),
                            LookupService.GEOIP_MEMORY_CACHE );
                }
            }
            Location loc = cl.getLocation(ip);
            return RowFactory.create(ip, loc!=null?loc.countryCode:null, id);
        }
    }	
	
		 static class Transformer1 implements Function<String, Row> {
		        Pattern linePattern1 = Pattern.compile("(.*?).*?\\[(.*?)\\].*");
		        //Pattern linePattern2 = Pattern.compile(".*?id(?:=|%3D)(.*)?");
		        static LookupService cl;
		        static Object lock = new Object();
		        @Override
		        public Row call(String line) throws Exception {
		            Matcher m1 = linePattern1.matcher(line);
		            String ip = null;
		            //String id = null;
		            String dt = null;
		            int hour = -1;
		            if (m1.find()) {
		                ip = m1.group(1);
		                dt = m1.group(2);
		                //java.util.Calendar formatdt = new GregorianCalendar();
		                //Date date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +SSSS", Locale.ENGLISH).parse(dt);
		                //formatdt.setTime(date);
		                //hour = formatdt.get(Calendar.HOUR_OF_DAY);
		               /* Matcher m2 = linePattern2.matcher(url);
		                if (m2.find()) {
		                    id = m2.group(1);
		                }*/
		            }
		            synchronized(lock) {
		                if (cl == null) {
		                    cl = new LookupService(SparkFiles.get("GeoLiteCity.dat"),
		                            LookupService.GEOIP_MEMORY_CACHE );
		                }
		            }
		            Location loc = cl.getLocation(ip);
		            //return RowFactory.create(ip, loc!=null?loc.countryCode:null, id);
		            return RowFactory.create(ip, loc!=null?loc.countryCode:null,hour);
		        }
		    }
		    
			public static void main(String[] args) throws IOException {
				SparkSession spark = SparkSession
		                .builder()
		                .appName(PeakAmazonTime.class.getName())
		                .getOrCreate();

				JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
				JavaRDD<Row> accessLogRDD = context.textFile("access_log_sample")
				         .filter(line -> line.matches(".*&url=(https:|http:|http%3A|https%3A)//www.amazon.com.*"))
				         .map(new Transformer1());

				         accessLogRDD.saveAsTextFile("output");

				List<StructField> accessLogFields = new ArrayList<>();
				accessLogFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
				accessLogFields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
				//accessLogFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
				accessLogFields.add(DataTypes.createStructField("hour", DataTypes.IntegerType, true));
		        StructType accessLogType = DataTypes.createStructType(accessLogFields);

		        Dataset<Row> accessLogDf = spark.createDataFrame(accessLogRDD, accessLogType)
		                .distinct()
		                .where("country = 'US'");


//				JavaPairRDD<Text, ParseData> inputRDD = context.sequenceFile("s3a://daijytest/nutchdb/segments/*/parse_data/part-*/data", Text.class, ParseData.class);
//				JavaRDD<Tuple2<String, String>> appMetaRDD = inputRDD.map(pair -> new Tuple2(pair._1.toString(), pair._2.getMeta("category")));

				/*JavaRDD<String> inputRDD = context.textFile("appmetadata.txt.gz");
				JavaRDD<Tuple2<String, String>> appMetaRDD = inputRDD.map(
				        new Function<String, Tuple2<String, String>>() {
				            @Override
				            public Tuple2<String, String> call(String line) {
				                String[] items = line.split("\t");
				                String url = items.length>0?items[0]:null;
				                String category = items.length>5?items[5]:null;
				                return new Tuple2(url, category);
				            }
				        });

				final Pattern linePattern2 = Pattern.compile(".*?id=(.*)?");
				JavaRDD<Row> idCategoryRDD = appMetaRDD.map(new Function<Tuple2<String, String>, Row>() {
				    @Override
			        public Row call(Tuple2<String, String> t) throws Exception {
				        Matcher m = linePattern2.matcher(t._1);
				        String id = null;
				        if (m.find()) {
				            id = m.group(1);
				        }
				        return RowFactory.create(id, t._2);
				    }
				});

				List<StructField> appMetaFields = new ArrayList<>();
				appMetaFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
				appMetaFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
		        StructType appMetaType = DataTypes.createStructType(appMetaFields);

				Dataset<Row> appMetaDf = spark.createDataFrame(idCategoryRDD, appMetaType);*/

				/*accessLogDf.join(appMetaDf, "id")
				    .groupBy("category")
				    .agg(functions.count("*").as("c"))
				    .sort(functions.desc("c"))
				    .write(）
				    .csv("output");*/
			   // accessLogDf.groupBy("hour")
			    // .agg(functions.count("*").as("c"))
			    // .sort(functions.desc("c"))
			    // .write()
			    // .csv("output");
			}
	

}
