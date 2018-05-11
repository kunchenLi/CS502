package io.bittiger.adindex;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
/**
 * Created by jiayangan on 5/14/17.
 */

public class AdsSelector {
    private static AdsSelector instance = null;
    //private int EXP = 7200;
    private int numDocs = 10840;
    private String mMemcachedServer;
    private int mMemcachedPortal;
    private int mTFMemcachedPortal;
    private int mDFMemcachedPortal;
    private int mClickFeaturePortal;

    private String mysql_host;
    private String mysql_db;
    private String mysql_user;
    private String mysql_pass;
    private String m_logistic_reg_model_file;
    private String m_gbdt_model_path;
    private Boolean enableTFIDF;
    MemcachedClient cache;
    MemcachedClient tfCacheClient;
    MemcachedClient dfCacheClient;
    MemcachedClient clickFeatureCacheClient;
    MySQLAccess mysql;

    protected AdsSelector(String memcachedServer,int memcachedPortal,int tfMemcachedPortal, int dfMemcachedPortal,
                          int clickMemcachedPortal,
                          String logistic_reg_model_file,
                          String gbdt_model_path, String mysqlHost,String mysqlDb,String user,String pass)
    {
        mMemcachedServer = memcachedServer;
        mMemcachedPortal = memcachedPortal;
        mTFMemcachedPortal = tfMemcachedPortal;
        mDFMemcachedPortal = dfMemcachedPortal;
        mClickFeaturePortal = clickMemcachedPortal;
        m_logistic_reg_model_file = logistic_reg_model_file;
        m_gbdt_model_path = gbdt_model_path;
        mysql_host = mysqlHost;
        mysql_db = mysqlDb;
        mysql_user = user;
        mysql_pass = pass;
        String address = mMemcachedServer + ":" + mMemcachedPortal;
        try {
            cache = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(address));
        }catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        enableTFIDF = true;
        String tf_address = mMemcachedServer + ":" + mTFMemcachedPortal;
        String df_address = mMemcachedServer + ":" + mDFMemcachedPortal;
        String click_address = mMemcachedServer + ":" + mClickFeaturePortal;
        try
        {
            mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
            clickFeatureCacheClient = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(click_address));
            tfCacheClient = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(tf_address));
            dfCacheClient = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(df_address));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static AdsSelector getInstance(String memcachedServer,int memcachedPortal,int tfMemcachedPortal, int dfMemcachedPortal, int clickMemcachedPortal, String logistic_reg_model_file,
                                          String gbdt_model_path,String mysqlHost,String mysqlDb,String user,String pass) {
        if(instance == null) {
            instance = new AdsSelector(memcachedServer, memcachedPortal, tfMemcachedPortal, dfMemcachedPortal,clickMemcachedPortal,logistic_reg_model_file, gbdt_model_path,mysqlHost,mysqlDb,user,pass);
        }
        return instance;
    }

    public List<Ad> selectAds(Query query,String deviceId, String deviceIp)
    {
        List<Ad> adList = new ArrayList<Ad>();
        HashMap<Long,Integer> matchedAds = new HashMap<Long,Integer>();
        try {
            List<String> queryTerms = new ArrayList<String>();

            for(int i = 0; i < query.getTermList().size();i++)
            {
                String queryTerm = query.getTerm(i);
                queryTerms.add(queryTerm);
                System.out.println("selectAds queryTerm = " + queryTerm);
                @SuppressWarnings("unchecked")
                //retrieve ads from index server by term
                Set<Long>  adIdList = (Set<Long>)cache.get(queryTerm);
                if(adIdList != null && adIdList.size() > 0)
                {
                    for(Object adId : adIdList)
                    {
                        Long key = (Long)adId;
                        if(matchedAds.containsKey(key))
                        {
                            int count = matchedAds.get(key) + 1;
                            matchedAds.put(key, count);
                        }
                        else
                        {
                            matchedAds.put(key, 1);
                        }
                    }
                }
            }
            int numOfQueryTerms = queryTerms.size();
            //add count for number of ads selected from memcached
            for(Long adId:matchedAds.keySet())
            {
                System.out.println("selectAds adId = " + adId);
                Ad.Builder  ad = mysql.getAdData(adId);
                double relevanceScore = matchedAds.get(adId) * 1.0 / ad.getKeyWordsList().size();
                //IDF score * TF score * fieldNorms = log(numDocs / (docFreq + 1)) * sqrt(tf) * (1/sqrt(length))
                double relevanceScoreTFIDF = getRelevanceScoreByTFIDF(adId, ad.getKeyWordsList().size(), query.getTermList());
                if(enableTFIDF) {
                    //filter logic: only keep ad contains all query terms
                    if (numOfQueryTerms != matchedAds.get(adId)) {
                        //L0 filter
                        continue;
                    }
                    ad.setRelevanceScore(relevanceScoreTFIDF);
                } else {
                    //we don't need filter logic here: relevanceScore only depends on number of keywords in ad
                    ad.setRelevanceScore(relevanceScore);
                }
                System.out.println("relevanceScore = " + relevanceScore);
                //ad.setRelevanceScore(relevanceScore);
                System.out.println("selectAds pClick = " + ad.getPClick());
                System.out.println("selectAds relevanceScore = " + ad.getRelevanceScore());
                //update pClick
                Ad ad_ = ad.build();
                //ad.setPClick(0.9);
                ad.setPClick(predictCTR(ad_, queryTerms, deviceId, deviceIp, true, clickFeatureCacheClient));
                adList.add(ad.build());
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return adList;
    }
    //TF*IDF = log(numDocs / (docFreq + 1)) * sqrt(tf) * (1/sqrt(docLength))
    private double calculateTFIDF(Long adId, String term, int docLength) {
        String tfKey = adId.toString() + "_" + term;
        System.out.println("tfKey = " + tfKey);
        System.out.println("dfKey = " + term);

        String tf = (String)tfCacheClient.get(tfKey);
        System.out.println("tf = " + tf);

        String df =  (String)dfCacheClient.get(term);
        System.out.println("df=" + df);

        if(tf != null && df != null) {
            int tfVal = Integer.parseInt(tf);
            int dfVal = Integer.parseInt(df);
            double dfScore = Math.log10(numDocs * 1.0 / (dfVal + 1));
            double tfScore = Math.sqrt(tfVal);
            double norm = Math.sqrt(docLength);
            double tfidfScore = (dfScore*tfScore) / norm;
            return tfidfScore;
        }
        return 0.0;
    }

    private double getRelevanceScoreByTFIDF(Long adId, int numOfKeyWords, List<String> queryTerms) {
        double relevanceScore = 0.0;
        for(String term : queryTerms) {
            relevanceScore += calculateTFIDF(adId, term, numOfKeyWords);
        }
        relevanceScore = relevanceScore * 1.0 / queryTerms.size();
        return relevanceScore;
    }

    private double predictCTR(Ad ad, List<String> queryTerms, String device_id, String device_ip,
                              boolean query_category_matched, MemcachedClient featureCacheClient) {
        //construct features, note that the order of features must be as follows
        ArrayList<Double> features = new ArrayList<Double>();
        //device_ip_click
        String device_ip_click_key = "dipc_" + device_ip;
        @SuppressWarnings("unchecked")
        String device_ip_click_val_str = (String)featureCacheClient.get(device_ip_click_key);
        Double device_ip_click_val = 0.0;
        if (device_ip_click_val_str != null && device_ip_click_val_str!= "") {
            device_ip_click_val = Double.parseDouble(device_ip_click_val_str);
        }
        features.add(device_ip_click_val);

        System.out.println("device_ip_click_key = " + device_ip_click_key);
        System.out.println("device_ip_click_val = " + device_ip_click_val);

        //device_ip_impression
        String device_ip_impression_key = "dipi_" + device_ip;
        @SuppressWarnings("unchecked")
        String device_ip_impression_val_str = (String)featureCacheClient.get(device_ip_impression_key);
        Double device_ip_impression_val = 0.0;
        if (device_ip_impression_val_str != null && device_ip_impression_val_str!= "") {
            device_ip_impression_val = Double.parseDouble(device_ip_impression_val_str);
        }
        features.add(device_ip_impression_val);
        //System.out.println("device_ip_impression_key = " + device_ip_impression_key);
        System.out.println("device_ip_impression_val = " + device_ip_impression_val);


        //device_id_click
        String device_id_click_key = "didc_" + device_id;
        @SuppressWarnings("unchecked")
        String device_id_click_val_str = (String)featureCacheClient.get(device_id_click_key);
        Double device_id_click_val = 0.0;
        if (device_id_click_val_str != null && device_id_click_val_str!= "") {
            device_id_click_val = Double.parseDouble(device_id_click_val_str);
        }
        features.add(device_id_click_val);
        System.out.println("device_id_click_key = " + device_id_click_key);
        System.out.println("device_id_click_val = " + device_id_click_val);
        System.out.println("device_id_click_val_str = " + device_id_click_val_str);

        //device_id_impression
        String device_id_impression_key = "didi_" + device_id;
        @SuppressWarnings("unchecked")
        String device_id_impression_val_str = (String)featureCacheClient.get(device_id_impression_key);
        Double device_id_impression_val = 0.0;
        if (device_id_impression_val_str != null && device_id_impression_val_str!= "") {
            device_id_impression_val = Double.parseDouble(device_id_impression_val_str);
        }
        features.add(device_id_impression_val);
        System.out.println("device_id_impression_key = " + device_id_impression_key);
        System.out.println("device_id_impression_val = " + device_id_impression_val);

        //ad_id_click

        String ad_id_click_key = "aidc_" + Long.toString(ad.getAdId());
        @SuppressWarnings("unchecked")
        String ad_id_click_val_str = (String)featureCacheClient.get(ad_id_click_key);
        Double ad_id_click_val = 0.0;
        if (ad_id_click_val_str != null && ad_id_click_val_str!= "") {
            ad_id_click_val = Double.parseDouble(ad_id_click_val_str);
        }
        features.add(ad_id_click_val);

        //ad_id_impression
        String ad_id_impression_key = "aidi_" + Long.toString(ad.getAdId());
        @SuppressWarnings("unchecked")
        String ad_id_impression_val_str = (String)featureCacheClient.get(ad_id_impression_key);
        Double ad_id_impression_val = 0.0;
        if (ad_id_impression_val_str != null && ad_id_impression_val_str!= "") {
            ad_id_impression_val = Double.parseDouble(ad_id_impression_val_str);
        }
        features.add(ad_id_impression_val);

        String query = Utility.strJoin(queryTerms, "_");
        //query_campaign_id_click
        String query_campaign_id_click_key = "qcidc_" + query + "_" + Long.toString(ad.getCampaignId());
        @SuppressWarnings("unchecked")
        String query_campaign_id_click_val_str = (String)featureCacheClient.get(query_campaign_id_click_key);
        Double query_campaign_id_click_val = 0.0;
        if (query_campaign_id_click_val_str != null && query_campaign_id_click_val_str!= "") {
            query_campaign_id_click_val = Double.parseDouble(query_campaign_id_click_val_str);
        }
        features.add(query_campaign_id_click_val);

        //query_campaign_id_impression
        String query_campaign_id_impression_key = "qcidi_" + query + "_" + Long.toString(ad.getCampaignId());
        @SuppressWarnings("unchecked")
        String query_campaign_id_impression_val_str = (String)featureCacheClient.get(query_campaign_id_impression_key);
        Double query_campaign_id_impression_val = 0.0;
        if (query_campaign_id_impression_val_str != null && query_campaign_id_impression_val_str!= "") {
            query_campaign_id_impression_val = Double.parseDouble(query_campaign_id_impression_val_str);
        }
        features.add(query_campaign_id_impression_val);

        //query_ad_id_click
        String query_ad_id_click_key = "qaidc_" + query + "_" + Long.toString(ad.getAdId());
        @SuppressWarnings("unchecked")
        String query_ad_id_click_val_str = (String)featureCacheClient.get(query_ad_id_click_key);
        Double query_ad_id_click_val = 0.0;
        if (query_ad_id_click_val_str != null && query_ad_id_click_val_str!= "") {
            query_ad_id_click_val = Double.parseDouble(query_ad_id_click_val_str);
        }
        features.add(query_ad_id_click_val);

        //query_ad_id_impression
        String query_ad_id_impression_key = "qaidi_" + query + "_" + Long.toString(ad.getAdId());
        @SuppressWarnings("unchecked")
        String query_ad_id_impression_val_str = (String)featureCacheClient.get(query_ad_id_impression_key);
        Double query_ad_id_impression_val = 0.0;
        if (query_ad_id_impression_val_str != null && query_ad_id_impression_val_str!= "") {
            query_ad_id_impression_val = Double.parseDouble(query_ad_id_impression_val_str);
        }
        features.add(query_ad_id_impression_val);

        //query_ad_category_match scale to 1000000 if match
        double query_ad_category_match = 0.0;
        if(query_category_matched) {
            query_ad_category_match = 1000000.0;
        }
        features.add(query_ad_category_match);
        //ad.toBuilder().setPClick()

        double pClick = CTRModel.getInstance(m_logistic_reg_model_file, m_gbdt_model_path).predictCTRWithLogisticRegression(features);
        System.out.println("ad.pClick = " + Double.toString(pClick));
        return pClick;
    }
}
