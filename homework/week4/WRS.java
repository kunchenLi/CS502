package io.bittiger.ad;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.packed.PackedInts;


public class WRS {
    public static Ad weightedRand(List<Ad> Ads, double[] weights) {
        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        Random r = new Random();
        double val = r.nextDouble() * sum;
        double total = 0;
        int i = 0;
        while (i < weights.length) {
            total += weights[i];
            if (val <= total) {
                break;
            }
            i++;
        }
        return Ads.get(i);
    }
}
