package io.bittiger.ads;

import java.util.List;

public class AdsAllocation {
	private static AdsAllocation instance = null;
	private static double mainLinePriceThreshold = 4.5;
	private static double mainLineRankScoreThreshold = 1.0;
	protected AdsAllocation()
	{

	}
	public static AdsAllocation getInstance() {
	      if(instance == null) {
	         instance = new AdsAllocation();
	      }
	      return instance;
	}
	public void AllocateAds(List<Ad> ads)
	{
		for(Ad ad : ads)
		{
			if(ad.costPerClick >= mainLinePriceThreshold && ad.rankScore >= mainLineRankScoreThreshold)
			{
				ad.position = 1;
			}
			else
			{
				ad.position = 2;
			}
		}
	}
}
