package com.tracker.storm.recsys;

import java.util.Map;

public class ModelBasedRec {
	
	
	public ModelBasedRec(){
	}

	/**
	 * def get_score_model02(mobile, mid, tar_mid_set, f2, f4, f7_mid, f7_usr, weight):
	f2_value = 0
	f4_value = 0
	f7_value = 0
	final_sim = 0
	intercept = weight['intercept']
	f2_weight = weight['f2']
	f4_weight = weight['f4']
	f7_weight = weight['f7']
	for to_mid in tar_mid_set:
		if (to_mid in f4) and (mid in f4[to_mid]):
			f4_value = string.atof(f4[to_mid][mid])
		if f4_value < weight['f4_thd']:
			continue
		if (mobile in f2) and (to_mid in f2[mobile]):
			f2_value = string.atof(f2[mobile][to_mid])
		if (mobile in f7_usr) and (to_mid in f7_mid):
			f7_value = (1.0*string.atof(f7_usr[mobile]))/string.atof(f7_mid[to_mid])
		score = intercept + f2_weight * f2_value + f4_weight * f4_value + f7_weight * f7_value
		final_score = 1.0 / (1 + math.exp(-score))
			
		if final_score > final_sim :
			final_sim = final_score
	if final_sim < weight['score_thd']:
		return 0
	return final_sim
	 */
	public float getLogitScore(int userid, int jobid, Map<String, Float> weight){
		float f1_value = 0;
		float f2_value = 0;
		float f3_value = 0;
		float final_sim = 0;
		float intercept = weight.get("intercept");
		float f1_weight = weight.get("f1");
		float f2_weight = weight.get("f2");
		float f3_weight = weight.get("f3");
		
		

		return 0;
	}
}
