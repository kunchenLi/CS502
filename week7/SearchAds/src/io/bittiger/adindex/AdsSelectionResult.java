package io.bittiger.adindex;
import java.util.List;

public class AdsSelectionResult {
	private java.util.List<io.bittiger.adindex.Ad> adsList;
	public AdsSelectionResult() {
		adsList = new java.util.ArrayList<io.bittiger.adindex.Ad>();
	}
	public synchronized void add(java.util.List<io.bittiger.adindex.Ad> _adsList){
		if (_adsList != null) {
			for(io.bittiger.adindex.Ad ad : _adsList) {
				adsList.add(ad);
			}
		}
	}
	public synchronized java.util.List<io.bittiger.adindex.Ad> getAdsList(){
		return adsList;
	}
}
