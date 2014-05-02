package org.eduonix;

import org.eduonix.clustering.MahoutRecommenderDataGenerator;
import org.eduonix.clustering.StoreRecommender;

/**
 * Created by ubu on 5/2/14.
 */
public class RecommenderRunner {

   static  int records = 200;


    public static void main(String[] args) throws Exception {


        MahoutRecommenderDataGenerator recommenderData = new MahoutRecommenderDataGenerator();

        recommenderData.setUpMockData(records);

        recommenderData.setUpPreferenceData();

        StoreRecommender recommender = new StoreRecommender();

        recommender.setUpUserId();

        recommender.phaseOneModel();

        recommender.phaseThreeModel();


    }
}
