package io.bittiger.ads;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

public class IndexBuilder {
	private int EXP = 0; //0: never expire
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	private MemcachedClient cache;
	
	public IndexBuilder(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
		String address = mMemcachedServer + ":" + mMemcachedPortal;
		try 
		{
			cache = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(address));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private int getRandPrice() {
		Random r = new Random();
		int Low = 10;
		int High = 100;
		int price = r.nextInt(High-Low) + Low;
		return price;
	}
	public Boolean buildInvertIndex(Ad ad)
	{
		String keyWords = Utility.strJoin(ad.keyWords, ",");
		List<String> tokens = Utility.cleanedTokenize(keyWords);
		for(int i = 0; i < tokens.size();i++)
		{
			String key = tokens.get(i);
			if(cache.get(key) instanceof Set)
			{
				@SuppressWarnings("unchecked")
				Set<Long>  adIdList = (Set<Long>)cache.get(key);
				adIdList.add(ad.adId);
			    cache.set(key, EXP, adIdList);
			}
			else
			{
				Set<Long>  adIdList = new HashSet<Long>();
				adIdList.add(ad.adId);
				cache.set(key, EXP, adIdList);
			}
		}
		return true;
	}
	//add position to posting list
	public Boolean buildInvertIndexV2(Ad ad)
	{
		String keyWords = Utility.strJoin(ad.keyWords, ",");
		List<String> tokens = Utility.cleanedTokenize(keyWords);
		for(int i = 0; i < tokens.size();i++)
		{
			String key = tokens.get(i);
			if(cache.get(key) instanceof Set)
			{
				@SuppressWarnings("unchecked")
				HashMap<Long, Set<Integer>>  adIdList = (HashMap<Long, Set<Integer>>)cache.get(key);
				if(adIdList.containsKey(ad.adId)) {
					adIdList.get(ad.adId).add(i);
				} else {
					Set<Integer> positions = new HashSet<Integer>();
					positions.add(i);
					adIdList.put(ad.adId, positions);					
				}
			    cache.set(key, EXP, adIdList);
			}
			else
			{ 
				//key: document Id, value: sorted position list
				HashMap<Long, Set<Integer>>  adIdList = new HashMap<Long, Set<Integer>>();
				Set<Integer> positions = new HashSet<Integer>();
				positions.add(i);
				adIdList.put(ad.adId, positions);
				cache.set(key, EXP, adIdList);
			}
		}
		return true;
	}
	public Boolean buildForwardIndex(Ad ad)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			if(ad.price == 0) {
				ad.price = getRandPrice();
			}
			mysql.addAdData(ad);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	public Boolean updateBudget(Campaign camp)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			mysql.addCampaignData(camp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
}
