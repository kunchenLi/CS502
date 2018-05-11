package io.bittiger.crawler;

/**
 * Created by john on 10/12/16.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import java.io.IOException;
import java.util.List;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.bittiger.ad.Ad;

public class CrawlerMain {
    public static void main(String[] args) throws IOException {
        if(args.length < 2)
        {
            System.out.println("Usage: Crawler <rawQueryDataFilePath> <adsDataFilePath> <proxyFilePath> <logFilePath>");
            System.exit(0);
        }
        ObjectMapper mapper = new ObjectMapper();
        String rawQueryDataFilePath = args[0];
        String adsDataFilePath = args[1];
        String proxyFilePath = args[2];
        String logFilePath = args[3];
        AmazonCrawler crawler = new AmazonCrawler(proxyFilePath, logFilePath);
        File file = new File(adsDataFilePath);
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        try (BufferedReader br = new BufferedReader(new FileReader(rawQueryDataFilePath))) {

            String line;
            while ((line = br.readLine()) != null) {
                if(line.isEmpty())
                    continue;
                System.out.println(line);
                //home theater system , 22.0, 8060,11
                String[] fields = line.split(",");
                String query = fields[0].trim();
                double bidPrice = Double.parseDouble(fields[1].trim());
                int campaignId = Integer.parseInt(fields[2].trim());
                int queryGroupId = Integer.parseInt(fields[3].trim());
                int startIndex = 0;
                for(Integer pageNum = 1; pageNum <= 10; pageNum++) {
                    List<Ad> ads = crawler.GetAdBasicInfoByQuery(query, bidPrice, campaignId, queryGroupId, pageNum,startIndex);
                    for (Ad ad : ads) {
                        String jsonInString = mapper.writeValueAsString(ad);
                        //System.out.println(jsonInString);
                        bw.write(jsonInString);
                        bw.newLine();
                    }
                    startIndex = startIndex + ads.size();
                    Thread.sleep(4000);
                }
            }
            bw.close();
        }catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        crawler.cleanup();
    }
}
