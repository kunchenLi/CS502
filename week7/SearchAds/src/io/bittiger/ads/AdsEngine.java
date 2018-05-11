package io.bittiger.ads;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.json.*;

import io.bittiger.adindex.AdsIndexClientWorker;
import io.bittiger.adindex.AdsSelectionResult;

public class AdsEngine {
	private String mAdsDataFilePath;
	private String mBudgetFilePath;
	String m_logistic_reg_model_file;
	String m_gbdt_model_path;
	private IndexBuilder indexBuilder;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private int mFeatureMemcachedPortal;
	private int mSynonymsMemcachedPortal;
	private int mTFMemcachedPortal;
	private int mDFMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	private Boolean enable_query_rewrite;
	private int indexServerTimeout; //ms
	
	public AdsEngine(String adsDataFilePath, String budgetDataFilePath,String logistic_reg_model_file, 
			String gbdt_model_path, String memcachedServer,int memcachedPortal,int featureMemcachedPortal,int synonymsMemcachedPortal,
			int tfMemcachedPortal, int dfMemcachedPortal,
			String mysqlHost,String mysqlDb,String user,String pass)
	{
		mAdsDataFilePath = adsDataFilePath;
		mBudgetFilePath = budgetDataFilePath;
		m_logistic_reg_model_file = logistic_reg_model_file;
		m_gbdt_model_path = gbdt_model_path;
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mTFMemcachedPortal = tfMemcachedPortal;
		mDFMemcachedPortal = dfMemcachedPortal;
		mFeatureMemcachedPortal = featureMemcachedPortal;
		mSynonymsMemcachedPortal = synonymsMemcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
		enable_query_rewrite = false;
		indexServerTimeout = 50;
		indexBuilder = new IndexBuilder(memcachedServer,memcachedPortal,mysql_host,mysql_db,mysql_user,mysql_pass);
	}
	
	public Boolean init()
	{
		//load ads data
		try (BufferedReader brAd = new BufferedReader(new FileReader(mAdsDataFilePath))) {
			String line;
			while ((line = brAd.readLine()) != null) {
				JSONObject adJson = new JSONObject(line);
				Ad ad = new Ad(); 
				if(adJson.isNull("adId") || adJson.isNull("campaignId")) {
					continue;
				}
				ad.adId = adJson.getLong("adId");
				ad.campaignId = adJson.getLong("campaignId");
				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
				ad.keyWords = new ArrayList<String>();
				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
				for(int j = 0; j < keyWords.length();j++)
				{
					ad.keyWords.add(keyWords.getString(j));
				}
				if(!indexBuilder.buildInvertIndex(ad)) {
					
			    }
				
//				if(!indexBuilder.buildInvertIndex(ad) || !indexBuilder.buildForwardIndex(ad))
//				{
//					//log				
//				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//load budget data
		try (BufferedReader brBudget = new BufferedReader(new FileReader(mBudgetFilePath))) {
			String line;
			while ((line = brBudget.readLine()) != null) {
				JSONObject campaignJson = new JSONObject(line);
				Long campaignId = campaignJson.getLong("campaignId");
				double budget = campaignJson.getDouble("budget");
				Campaign camp = new Campaign();
				camp.campaignId = campaignId;
				camp.budget = budget;
				if(!indexBuilder.updateBudget(camp))
				{
					//log
				}			
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	private Ad CloneAd(io.bittiger.adindex.Ad ad){
		Ad result = new Ad(); 
		result.adId = ad.getAdId();
		result.campaignId = ad.getCampaignId();
    	int keyWordsSize = ad.getKeyWordsList().size();
    	result.keyWords = ad.getKeyWordsList().subList(0, keyWordsSize);
    	result.relevanceScore = ad.getRankScore();
    	result.pClick = ad.getPClick();
    	result.bidPrice = ad.getBidPrice();
    	result.rankScore = ad.getRankScore();
    	result.qualityScore = ad.getQualityScore();
    	result.costPerClick = ad.getCostPerClick();
    	result.position = ad.getPosition();
    	result.title = ad.getTitle();
    	result.price = ad.getPrice();
    	result.thumbnail = ad.getThumbnail();
    	result.description = ad.getDescription();
    	result.brand = ad.getBrand();
    	result.detail_url = ad.getDetailUrl();
    	result.query = ad.getQuery();
    	result.category = ad.getCategory();   
    	return result;
    }
	private AdsSelectionResult getAdsFromIndexServer(List<String> queryTerms, String deviceId, String deviceIp) {
		AdsSelectionResult adsResult = new AdsSelectionResult();
		io.bittiger.adindex.Query.Builder _query =  io.bittiger.adindex.Query.newBuilder();
		for(int i = 0; i< queryTerms.size();i++) {
			System.out.println("term = " + queryTerms.get(i));
			_query.addTerm(queryTerms.get(i));
		}
		System.out.println("term count= " + _query.getTermCount());	
		java.util.List<io.bittiger.adindex.Query> queryList = new ArrayList<io.bittiger.adindex.Query>();
		queryList.add(_query.build());
		//design choice
		//#1 sequentially call index server 1, 2, 3,4,5,6...
		//#2 parallel call index server 1, 2, 3,4,5,6..
		
		//design choice
		//#1 synchronized method to update, get adsResult
		//#2 each thread return a adsList, then we aggregate after threads join main thread
		AdsIndexClientWorker adsIndexClient1 = new AdsIndexClientWorker(queryList, "127.0.0.1",50051,deviceId,deviceIp, adsResult);
		AdsIndexClientWorker adsIndexClient2 = new AdsIndexClientWorker(queryList, "127.0.0.1",50052,deviceId,deviceIp, adsResult);
		adsIndexClient1.start();
		adsIndexClient2.start();
		//design choice
		//index server 1, 2, 3,4,5 return ads in 20 ms, index server 6 take 1000 ms
		//#1 set time out for each request
		//#2 wait for every index server request return	
		try {
			//adsIndexClient1.join();
			adsIndexClient1.join(indexServerTimeout);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			adsIndexClient2.join(indexServerTimeout);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//design choice
		//#1 aggregate ads from each index server then put them in one data structure, con: aggregation multiple arrays is expensive
		//#2 use concurrent utility , for example, synchronized data structure which is shared by threads, con：synchronized has cost (lock)		
		return adsResult;
	}
	public List<Ad> selectAds(String query, String device_id, String device_ip)
	{
		//query understanding
		//raw query: nike running shoe
		//running == jogging
		//nike running shoe => nike jogging shoe	
		List<Ad> adsCandidates = new ArrayList<Ad>();
		if (enable_query_rewrite) {
			//get rewrite from offline
			List<List<String>> rewrittenQuery =  QueryParser.getInstance().OfflineQueryRewrite(query, mMemcachedServer, mSynonymsMemcachedPortal);		
			
			//get rewrite from online
			if (rewrittenQuery.size() == 0) {
				List<String> queryTermList = QueryParser.getInstance().QueryUnderstand(query);
				//set timeout for OnlineQueryRewrite
				//estimation (low cost: n-gram, lookup):
				//how much % bi-gram we have exiting rewrite query, if % < 40, no OnlineQueryRewrite call
				rewrittenQuery =  QueryParser.getInstance().OnlineQueryRewrite(queryTermList, mMemcachedServer, mSynonymsMemcachedPortal);	
			}
	

			Set<Long> uniquueAds = new HashSet<Long>();		
			//select ads candidates for each rewritten Query
			//TODO: mult-thread call for each rewritten Query
			for (List<String> queryTerms : rewrittenQuery) {	
				AdsSelectionResult adsResult = getAdsFromIndexServer(queryTerms, device_id, device_ip);
				//convert ads
				for(io.bittiger.adindex.Ad _ad : adsResult.getAdsList()) {
					//System.out.println("relevance score = " + _ad.getRelevanceScore());
					//dedupe ads
					if (!uniquueAds.contains(_ad.getAdId())) {
						Ad ad = new Ad(); 
						ad.CloneAd(_ad);
						//System.out.println("relevance score = " + ad.relevanceScore);
						adsCandidates.add(ad);
					}
				}
//				List<Ad> adsCandidates_temp = AdsSelector.getInstance(mMemcachedServer, mMemcachedPortal,mFeatureMemcachedPortal,mTFMemcachedPortal,mDFMemcachedPortal,
//						m_logistic_reg_model_file,m_gbdt_model_path, mysql_host, mysql_db,mysql_user, mysql_pass).selectAds(queryTerms,device_id, device_ip, query_category);	
//				for(Ad ad : adsCandidates_temp) {
//					if (!uniquueAds.contains(ad.adId)) {
//						adsCandidates.add(ad);
//					}
//				}
			}
			
			//TODO,optional: give ads selected by rewritten query lower rank score
			

		} else {
			List<String> queryTerms = QueryParser.getInstance().QueryUnderstand(query);
			AdsSelectionResult adsResult = getAdsFromIndexServer(queryTerms, device_id, device_ip);
			System.out.println("Number of  ads from index server = " + adsResult.getAdsList().size());
			//convert ads
			for(io.bittiger.adindex.Ad _ad : adsResult.getAdsList()) {
				System.out.println("relevance score = " + _ad.getRelevanceScore());
				Ad ad = new Ad(); 
				ad.CloneAd(_ad);
				System.out.println("relevance score = " + ad.relevanceScore);
				adsCandidates.add(ad);
			}
			System.out.println("Number of adsCandidates = " + adsCandidates.size());

			//adsCandidates = AdsSelector.getInstance(mMemcachedServer, mMemcachedPortal,mFeatureMemcachedPortal,mTFMemcachedPortal, mDFMemcachedPortal,
				//m_logistic_reg_model_file,m_gbdt_model_path, mysql_host, mysql_db,mysql_user, mysql_pass).selectAds(queryTerms,device_id, device_ip, query_category);			
		}
			
		//L0 filter by pClick, relevance score
		//List<Ad> L0unfilteredAds = AdsFilter.getInstance().LevelZeroFilterAds(adsCandidates);
		//System.out.println("L0unfilteredAds ads left = " + L0unfilteredAds.size());
         
		//dedupe
		
		//rank 
		List<Ad> rankedAds = AdsRanker.getInstance().rankAds(adsCandidates);
		System.out.println("rankedAds ads left = " + rankedAds.size());

		//L1 filter by relevance score : select top K ads
		int k = 50;
		List<Ad> unfilteredAds = AdsFilter.getInstance().LevelOneFilterAds(rankedAds,k);
		System.out.println("unfilteredAds ads left = " + unfilteredAds.size());

		//Dedupe ads per campaign
		List<Ad> dedupedAds = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).DedupeByCampaignId(unfilteredAds);
	    System.out.println("dedupedAds ads left = " + dedupedAds.size());

		//pricing： next rank score/current score * current bid price
		AdPricing.getInstance().setCostPerClick(dedupedAds);
		//filter last one , ad without budget , ads with CPC < minReservePrice
		List<Ad> ads = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).ApplyBudget(dedupedAds);
		System.out.println("AdsCampaignManager ads left = " + ads.size());

		//allocation
		AdsAllocation.getInstance().AllocateAds(ads);
		return ads;
	}
}
