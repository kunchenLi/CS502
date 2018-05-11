package io.bittiger.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.net.*;

//import org.apache.log4j.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import io.bittiger.ad.Ad;
import io.bittiger.ad.Utility;

/**
 * Created by john on 10/13/16.
 */


public class AmazonCrawler {
    //https://www.amazon.com/s/ref=nb_sb_noss?field-keywords=nikon+SLR&page=2
    private static final String AMAZON_QUERY_URL = "https://www.amazon.com/s/ref=nb_sb_noss?field-keywords=";
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";
    private final String authUser = "bittiger";
    private final String authPassword = "cs504";
    private List<String> proxyList;
    private List<String> titleList;
    private List<String> categoryList;
    private List<String> detailUrlList;

    private HashSet crawledUrl;
    private int adId;

    BufferedWriter logBFWriter;

    private int index = 0;

    public AmazonCrawler(String proxy_file, String log_file) {
        crawledUrl = new HashSet();
        adId = 5000;
        initProxyList(proxy_file);

        initHtmlSelector();

        initLog(log_file);

    }

    public void cleanup() {
        if (logBFWriter != null) {
            try {
                logBFWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //raw url: https://www.amazon.com/KNEX-Model-Building-Set-Engineering/dp/B00HROBJXY/ref=sr_1_14/132-5596910-9772831?ie=UTF8&qid=1493512593&sr=8-14&keywords=building+toys
    //normalizedUrl: https://www.amazon.com/KNEX-Model-Building-Set-Engineering/dp/B00HROBJXY
    private String normalizeUrl(String url) {
        int i = url.indexOf("ref");
        String normalizedUrl = url.substring(0, i - 1);
        return normalizedUrl;
    }

    private void initProxyList(String proxy_file) {
        proxyList = new ArrayList<String>();
        try (BufferedReader br = new BufferedReader(new FileReader(proxy_file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String ip = fields[0].trim();
                proxyList.add(ip);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Authenticator.setDefault(
                new Authenticator() {
                    @Override
                    public PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                authUser, authPassword.toCharArray());
                    }
                }
        );

        System.setProperty("http.proxyUser", authUser);
        System.setProperty("http.proxyPassword", authPassword);
        System.setProperty("socksProxyPort", "61336"); // set proxy port
    }

    private void initHtmlSelector() {
        titleList = new ArrayList<String>();
        titleList.add(" > div > div:nth-child(3) > div.a-row.a-spacing-top-mini > a > h2");
        titleList.add(" > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1)  > a > h2");
        titleList.add(" > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a > h2");
        //#result_157 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a
        categoryList = new ArrayList<String>();
        //#refinements > div.categoryRefinementsSection > ul.forExpando > li:nth-child(1) > a > span.boldRefinementLink
        categoryList.add("#refinements > div.categoryRefinementsSection > ul.forExpando > li > a > span.boldRefinementLink");
        categoryList.add("#refinements > div.categoryRefinementsSection > ul.forExpando > li:nth-child(1) > a > span.boldRefinementLink");
        //#leftNavContainer > ul:nth-child(3) > div > li:nth-child(1) > span > a > h4
        //#leftNavContainer > ul:nth-child(3) > div > li:nth-child(1) > span > ul > div > li:nth-child(2) > span > a > span
        detailUrlList = new ArrayList<String>();
        detailUrlList.add(" > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a");
        detailUrlList.add(" > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a");
        detailUrlList.add(" > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a");
    }

    private void initLog(String log_path) {
        try {
            File log = new File(log_path);
            // if file doesnt exists, then create it
            if (!log.exists()) {
                log.createNewFile();
            }
            FileWriter fw = new FileWriter(log.getAbsoluteFile());
            logBFWriter = new BufferedWriter(fw);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void setProxy() {
        //rotate, round robbin
        if (index == proxyList.size()) {
            index = 0;
        }
        String proxy = proxyList.get(index);
        System.setProperty("socksProxyHost", proxy); // set proxy server
        index++;
    }

    private void testProxy() {
        System.setProperty("socksProxyHost", "199.101.97.146"); // set proxy server
        //System.setProperty("socksProxyPort", "61336"); // set proxy port
        String test_url = "http://www.toolsvoid.com/what-is-my-ip-address";
        try {
            Document doc = Jsoup.connect(test_url).userAgent(USER_AGENT).timeout(10000).get();
                                  //body > section.articles-section > div > div > div > div.col-md-8.display-flex > div > div.table-responsive > table > tbody > tr:nth-child(1) > td:nth-child(2) > strong
            String iP = doc.select("body > section.articles-section > div > div > div > div.col-md-8.display-flex > div > div.table-responsive > table > tbody > tr:nth-child(1) > td:nth-child(2) > strong").first().text(); //get used IP.
            System.out.println("IP-Address: " + iP);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public List<Ad> GetAdBasicInfoByQuery(String query, double bidPrice,int campaignId,int queryGroupId,Integer pageNum,int startIndex) {
        List<Ad> products = new ArrayList<>();
        try {
            if (false) {
                testProxy();
                return products;
            }

            setProxy();

            String url = AMAZON_QUERY_URL + query;
            if (pageNum > 1) {
                url = url  + "&page="  + pageNum.toString();
            }
            System.out.println("request_url = " + url);

            HashMap<String,String> headers = new HashMap<String,String>();
            headers.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
            //headers.put("Accept-Encoding", "gzip, deflate");
            headers.put("Accept-Language", "en-US,en;q=0.8");
            Document doc = Jsoup.connect(url).headers(headers).userAgent(USER_AGENT).timeout(100000).get();

            //Document doc = Jsoup.connect(url).userAgent(USER_AGENT).timeout(100000).get();

            //System.out.println(doc.text());
            //Elements results = doc.select("li");

            Elements results = doc.select("li[data-asin]");
            if (results.size() == 0) {
                logBFWriter.write("0 result for query :" + query + " , pageNum = " + pageNum.toString());
                logBFWriter.newLine();
            }

            System.out.println("num of results = " + results.size());
            Elements prods = doc.select("a[title][href]");
            System.out.println("num of results from dom = " + prods.size());

            for(int i = 0; i < results.size() ;i++) {
                Ad ad = new Ad();
                int index = startIndex + i;

                //detail url
                //#result_16 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a
                //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
                //#result_19 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a
                //#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a
                //#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a
                //#result_1 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a
                //#result_3 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a
                boolean crawled = false;
                for(String detail_path : detailUrlList) {
                    detail_path = "#result_" + Integer.toString(index) + detail_path;
                    Element detail_url_ele = doc.select(detail_path).first();
                    if (detail_url_ele != null) {
                        //System.out.println("detail_path = " + detail_path);
                        String detail_url = detail_url_ele.attr("href");
                        //System.out.println("detail = " + detail_url);
                        String normalizedUrl = normalizeUrl(detail_url);
                        if (crawledUrl.contains(normalizedUrl)) {
                            logBFWriter.write("crawled url:" + normalizedUrl);
                            logBFWriter.newLine();
                            crawled = true;
                            break;
                        }
                        crawledUrl.add(normalizedUrl);
                        System.out.println("normalized url  = " + normalizedUrl);
                        ad.detail_url = normalizedUrl;
                        break;
                    }
                }

                if(crawled) {
                    continue;
                }

                if(ad.detail_url == null || ad.detail_url == "") {
                    logBFWriter.write("cannot parse detail for query:" + query );
                    logBFWriter.newLine();
                    System.out.println("cannot parse detail for query:" + query );
                    continue;
                }

                ad.query = query;
                ad.query_group_id = queryGroupId;

                //title
                //#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a > h2
                //#result_3 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a > h2
                //#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a > h2
                //#result_1 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a > h2
                for (String title : titleList) {
                    String title_ele_path = "#result_" + Integer.toString(index) + title;
                    Element title_ele = doc.select(title_ele_path).first();
                    if (title_ele != null) {
                        System.out.println("title = " + title_ele.text());
                        ad.title = title_ele.text();
                        break;
                    }
                }

                if (ad.title == null || ad.title == "") {
                    logBFWriter.write("cannot parse title for query: " + query);
                    logBFWriter.newLine();
                    continue;
                }

                //keywords
                ad.keyWords = Utility.cleanedTokenize(ad.title);
                //#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img

                //thumbnail
                String thumbnail_path = "#result_" + Integer.toString(index) + " > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img";
                Element thumbnail_ele = doc.select(thumbnail_path).first();
                if (thumbnail_ele != null) {
                    //System.out.println("thumbnail = " + thumbnail_ele.attr("src"));
                    ad.thumbnail = thumbnail_ele.attr("src");
                } else {
                    logBFWriter.write("cannot parse thumbnail for query:" + query + ", title: " + ad.title);
                    logBFWriter.newLine();
                    continue;
                }

                //brand
                String brand_path = "#result_" + Integer.toString(index) + " > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div > span:nth-child(2)";
                Element brand = doc.select(brand_path).first();
                if (brand != null) {
                    //System.out.println("brand = " + brand.text());
                    ad.brand = brand.text();
                }
                //#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(3) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > span
                ad.bidPrice = bidPrice;
                ad.campaignId = campaignId;
                ad.price = 0.0;
                //#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(3) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > span

                //price
                String price_whole_path = "#result_" + Integer.toString(index) + " > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(3) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > span";
                String price_fraction_path = "#result_" + Integer.toString(index) + " > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(3) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > sup.sx-price-fractional";
                Element price_whole_ele = doc.select(price_whole_path).first();
                if (price_whole_ele != null) {
                    String price_whole = price_whole_ele.text();
                    //System.out.println("price whole = " + price_whole);
                    //remove ","
                    //1,000
                    if (price_whole.contains(",")) {
                        price_whole = price_whole.replaceAll(",", "");
                    }

                    try {
                        ad.price = Double.parseDouble(price_whole);
                    } catch (NumberFormatException ne) {
                        // TODO Auto-generated catch block
                        ne.printStackTrace();
                        //log
                    }
                }

                Element price_fraction_ele = doc.select(price_fraction_path).first();
                if (price_fraction_ele != null) {
                    //System.out.println("price fraction = " + price_fraction_ele.text());
                    try {
                        ad.price = ad.price + Double.parseDouble(price_fraction_ele.text()) / 100.0;
                    } catch (NumberFormatException ne) {
                        ne.printStackTrace();
                    }
                }
                //System.out.println("price = " + ad.price );

                //category
                for (String category : categoryList) {
                    Element category_ele = doc.select(category).first();
                    if (category_ele != null) {
                        //System.out.println("category = " + category_ele.text());
                        ad.category = category_ele.text();
                        break;
                    }
                }
                if (ad.category == "") {
                    logBFWriter.write("cannot parse category for query:" + query + ", title: " + ad.title);
                    logBFWriter.newLine();
                    continue;
                }
                ad.adId = adId;
                adId++;
                products.add(ad);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return products;
    }
}
