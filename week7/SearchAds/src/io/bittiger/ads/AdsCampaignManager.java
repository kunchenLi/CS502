package io.bittiger.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class AdsCampaignManager {
	private static AdsCampaignManager instance = null;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	private static double minPriceThreshold = 0.1;
	protected AdsCampaignManager(String mysqlHost,String mysqlDb,String user,String pass)
	{
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
	}
	public static AdsCampaignManager getInstance(String mysqlHost,String mysqlDb,String user,String pass) {
	      if(instance == null) {
	         instance = new AdsCampaignManager(mysqlHost,mysqlDb,user,pass);
	      }
	      return instance;
	}
	public  List<Ad> DedupeByCampaignId(List<Ad> adsCandidates)
	{
		List<Ad> dedupedAds = new ArrayList<Ad>();
		HashSet<Long> campaignIdSet = new HashSet<Long>();
		for(Ad ad : adsCandidates)
		{
			if(!campaignIdSet.contains(ad.campaignId))
			{
				dedupedAds.add(ad);
				campaignIdSet.add(ad.campaignId);
			}
		}
		return dedupedAds;
	}
	public List<Ad> ApplyBudget(List<Ad> adsCandidates)
	{
		List<Ad> ads = new ArrayList<Ad>();
		try
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			for(int i = 0; i < adsCandidates.size()  - 1;i++)
			{
				Ad ad = adsCandidates.get(i);
				Long campaignId = ad.campaignId;
				System.out.println("campaignId: " + campaignId);
				Double budget = mysql.getBudget(campaignId);
				System.out.println("AdsCampaignManager ad.costPerClick= " + ad.costPerClick);
				System.out.println("AdsCampaignManager campaignId= " + campaignId);
				System.out.println("AdsCampaignManager budget left = " + budget);
				//probelem:
				//#1 data consistency/correctness in database
				//#2 max concurrent update transactions supported by databases, oracle  
				//design choice
				//#1 transaction
				//#2 offline/Async process
				//ApplyBudget log: tractionId(sessionID), campaignId, ad.costPerClick , adId, budget_log_2017_11_11_14_00.txt, adId, budget_log_2017_11_11_16_00.txt
				//1.set budget buffer and alert on 10% of budget left on campaignId 9999 -> offline process campaignId 9999
				//2.read budget log every 2 hours
				//3.reduce offline process frequency 
				//4.aggregate cpc per campaignID then update budget for current campaign once
				//send transaction to message queue: q1 : 4 hours, q2: 2 hours, q3 : 30 mins, q4: 10 mins
				if(ad.costPerClick <= budget && ad.costPerClick >= minPriceThreshold)
				{
					ads.add(ad);
					budget = budget - ad.costPerClick;					
					mysql.updateCampaignData(campaignId, budget);
				}
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return ads;
	}
	
}
