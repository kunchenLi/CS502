package io.bittiger.ads;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AdsRanker {
	private static AdsRanker instance = null;
	private static double d;
	
	protected AdsRanker()
	{
		d = 0.25;
	}
	public static AdsRanker getInstance() {
	      if(instance == null) {
	         instance = new AdsRanker();
	      }
	      return instance;
	}
	public List<Ad> rankAds(List<Ad> adsCandidates)
	{
		for(Ad ad : adsCandidates)
		{
			ad.qualityScore = d * ad.pClick  +  (1.0 - d) * ad.relevanceScore;
			ad.rankScore = ad.qualityScore * ad.bidPrice;			
		}
		//sort by rank score
		Collections.sort(adsCandidates, new Comparator<Ad>() {
	        @Override
	        public int compare(Ad ad2, Ad ad1)
	        {
	        	if (ad1.rankScore < ad2.rankScore)
	        		return -1;
	        	else if(ad1.rankScore > ad2.rankScore)
	        		return 1;
	        	else
	        		return 0;
	        }
	    });
		
		for(Ad ad : adsCandidates)
		{
			System.out.println("ranker rankScore = " + ad.rankScore);		
		}
		return adsCandidates;
	}
}
