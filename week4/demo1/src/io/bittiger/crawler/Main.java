package io.bittiger.crawler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


public class Main {
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";
    private static final String USER_AGENT2 = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1";
    public static void main(String[] args) throws IOException {
        ArrayList<String> queryList = new ArrayList<String>();
        queryList.add("football");
        //queryList.add("Wall%20Mount%20Shelf");
        //queryList.add("iphone%20case");
        //queryList.add("building%20toys");
        //queryList.add("Radio%20Control%20Vehicle");
        //for(int k = 0; k < 1 ; k++){
            for(int i = 0; i < queryList.size();i++) {
                String requestUrl = "https://www.amazon.com/s/ref=nb_sb_noss?field-keywords=" + queryList.get(i);
                //syncCrawl(requestUrl);
                asyncCrawl(requestUrl);
            }
        //}

    }
    //detail url =/gp/slredirect/picassoRedirect.html/ref=pa_sp_atf_aps_sr_pg1_1?
    //ie=UTF8&adId=A03592132PFQ35AX2VPGJ
    //&url=https%3A%2F%2Fwww.amazon.com%2FObagi-ELASTIderm-Eye-Cream-0-5%2Fdp%2FB00303GLXY%2Fref%3Dsr_1_1%2F144-0591484-3075716%3Fie%3DUTF8%26qid%3D1504493034%26sr%3D8-1-spons%26keywords%3DEye%2BCream%26psc%3D1&qualifier=1504493034&id=3130580360972661&widgetName=sp_atf

    private static void syncCrawl(String requestUrl) throws IOException{
        Document doc = Jsoup.connect(requestUrl).userAgent(USER_AGENT).timeout(1000).get();

        //DOM
        /*
        Elements prods = doc.getElementsByAttribute("title");
        for(int i = 0;i < prods.size();i++) {
            System.out.println("prod title= " + prods.get(i).attr("title"));

        }*/
        //a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal
        Elements prodsLink = doc.getElementsByClass("a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal");
        System.out.println("num of prod link: " + prodsLink.size());
        for(int i = 0;i < prodsLink.size();i++) {
            //System.out.println("prod link= " + prodsLink.get(i).attr("href"));
            //System.out.println("prod title= " + prodsLink.get(i).attr("title"));
        }


        //data-asin
        Elements prodByAsin = doc.getElementsByAttribute("data-asin");
        //System.out.println("num of prod with asin: " + prodByAsin.size());

        //get by id, tips: prodsLink.size()




        //Selector

        String pageTitle = htmlTitle(doc);
        System.out.println("html title= " + pageTitle);

        List<String> urlElePaths = new ArrayList<String>();
        //#result_4 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
        //#result_4 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
        //#result_5 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
        //#result_7 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a
        //#result_8 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a
        urlElePaths.add("#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a");
        urlElePaths.add("#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a");

        //#result_1 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
        //#result_1
        //#result_2> div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.sx-line-clamp-2.s-list-title-medium > a > h2
        for(int j = 0;j < urlElePaths.size();j++) {
            Element ele = doc.select(urlElePaths.get(j)).first();
            if (ele != null) {
                String detailUrl = ele.attr("href");
                System.out.println("detail url = " + detailUrl);
                String title = ele.attr("title");
                System.out.println("title= " + title);
                break;
            }
        }
         //#result_# > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img
         //#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img
         //#result_10 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img
        //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img
        String imagePath = "#result_2 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img";
                            //result_1 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img
        Element imgEle = doc.select(imagePath).first();

        if(imgEle != null) {
            String imgUrl = imgEle.attr("src");
            System.out.println("image url = " + imgUrl);

        } else {

        }
        //String pricePath = "#result_6 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > span";
        //String fractionPricePath = "#result_6 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span > span > sup.sx-price-fractional";
        //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div:nth-child(1) > div:nth-child(3) > a > span > span
        //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div:nth-child(1) > div:nth-child(3) > a > span
        String priceLabel = "#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div:nth-child(1) > div:nth-child(3) > a > span.a-offscreen";
        //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div:nth-child(1) > div:nth-child(3) > a > span.a-offscreen
        Element priceEle = doc.select(priceLabel).first();
        if (priceEle != null) {
            String wholePriceStr = priceEle.text();
            System.out.println("PriceStr = " + wholePriceStr);
        }

    }

    //#blog_content > div > article > div.article-header > h1
    private static String htmlTitle(Document dom) {
        Element node = dom.select("title").first();
        if (node != null && node.text().length() > 0) {
            return node.text();
        }
        return null;
    }

    private static void asyncCrawl(String requestUrl) {
        try{
            //step1:  get response from http request url
            String response = sendHttpGetRequest(requestUrl);
            //System.out.println("Async html response= " + response);

            //async step2:store response in files or document based no SQL, hdfs

            //async step3: parse response with Jsoup
            Document doc = getDomFromContent(response, requestUrl);
            String pageTitle = htmlTitle(doc);
            System.out.println("Async html title= " + pageTitle);

            List<String> urlElePaths = new ArrayList<String>();
                           //#result_4 > div > div.a-row.a-spacing-top-small > div:nth-child(2) > a
                           //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
            //#result_2 > div > div.a-fixed-left-grid > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a
            urlElePaths.add("#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a");
            urlElePaths.add("#result_0 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div:nth-child(1) > a");
            //#result_4 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.sx-line-clamp-2.s-list-title-medium > a > h2

            for(int j = 0;j < urlElePaths.size();j++) {
                Element ele = doc.select(urlElePaths.get(j)).first();
                if (ele != null) {
                    String detailUrl = ele.attr("href");
                    System.out.println("Async detail url = " + detailUrl);
                    String title = ele.attr("title");
                    System.out.println("Async title= " + title);
                    break;
                }
            }

        } catch (Exception e){

        }
    }


    private static Document getDomFromContent(String content, String url) {
        return Jsoup.parse(content, url);
    }

    private static String sendHttpGetRequest(String url) throws Exception {
        URL urlObj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) urlObj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();
        int i = 0;

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
            i++;
        }
        System.out.println("number of line : " + Integer.toString(i));
        in.close();

        String responseStr = response.toString();
        return responseStr;
    }


}
