import java.util.*;

public List<String> nGram(String input, int n) {
		List<String> res = new ArrayList<>();
		if (input == null || input.length() == 0) {
			 return res;
		}
		String[] grams = input.split("\\s+");
		int left = 0;
		int right = n - 1;
		while (right < grams.length && left <= grams.length - n) {
			StringBuilder sb = new StringBuilder();
			for (int i = left; i <= right; i++) {
				sb.append(grams[i] + " ");
			}
			sb.deleteCharAt(sb.length() - 1);
			res.add(sb.toString());
			left++;
			right++;
		}
		return res;
}