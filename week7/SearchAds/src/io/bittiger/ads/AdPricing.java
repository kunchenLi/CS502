package io.bittiger.ads;

import java.util.List;

public class AdPricing {
	private static AdPricing instance = null;
	protected AdPricing()
	{

	}
	public static AdPricing getInstance() {
	      if(instance == null) {
	         instance = new AdPricing();
	      }
	      return instance;
	}
	public void setCostPerClick(List<Ad> adsCandidates)
	{
		for(int i = 0; i < adsCandidates.size();i++)
		{
			if(i < adsCandidates.size() - 1)
			{
				adsCandidates.get(i).costPerClick = adsCandidates.get(i + 1).rankScore / adsCandidates.get(i).qualityScore + 0.01;
			}
			else
			{
				adsCandidates.get(i).costPerClick = adsCandidates.get(i).bidPrice;
			}
		}
	}
}
