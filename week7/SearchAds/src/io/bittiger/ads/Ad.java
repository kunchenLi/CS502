package io.bittiger.ads;

import java.io.Serializable;
import java.util.List;

public class Ad implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Long adId;
	public Long campaignId;
	public List<String> keyWords;
	public double relevanceScore;
	public double pClick;	
	public double bidPrice;
	public double rankScore;
	public double qualityScore;
	public double costPerClick;
	public int position;//1: top , 2: bottom
    public String title; // required
    public double price; // required
    public String thumbnail; // required
    public String description; // required
    public String brand; // required
    public String detail_url; // required
    public String query; //required
    public String category;

    public void CloneAd(io.bittiger.adindex.Ad ad){
    	this.adId = ad.getAdId();
    	this.campaignId = ad.getCampaignId();
    	int keyWordsSize = ad.getKeyWordsList().size();
    	this.keyWords = ad.getKeyWordsList().subList(0, keyWordsSize);
    	this.relevanceScore = ad.getRelevanceScore();
    	this.rankScore = ad.getRankScore();
    	this.pClick = ad.getPClick();
    	this.bidPrice = ad.getBidPrice();
    	this.rankScore = ad.getRankScore();
    	this.qualityScore = ad.getQualityScore();
    	this.costPerClick = ad.getCostPerClick();
    	this.position = ad.getPosition();
    	this.title = ad.getTitle();
    	this.price = ad.getPrice();
    	this.thumbnail = ad.getThumbnail();
    	this.description = ad.getDescription();
    	this.brand = ad.getBrand();
    	this.detail_url = ad.getDetailUrl();
    	this.query = ad.getQuery();
    	this.category = ad.getCategory();   			
    }
}
