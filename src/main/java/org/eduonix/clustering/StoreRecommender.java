package org.eduonix.clustering;

import com.google.common.base.Splitter;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

/**
 * Created by ubu on 5/2/14.
 */
public class StoreRecommender {


    final static Logger log= LoggerFactory.getLogger(StoreRecommender.class);
    Path output = new Path("clustering/mockdata");

    String userId = null;

    String similarUserId = null;


    private static final Splitter SPLITTER = Splitter.onPattern("\\s+").omitEmptyStrings();

    public void setUpUserId() throws Exception {

        File file = new File(output.toString()+"/recommend.csv");
        try {
            Scanner scan = new Scanner(file);

            String line = scan.nextLine();

            List<String> tokens = SPLITTER.splitToList(line);

            userId = tokens.get(0);

            scan.next();

            String line2  = scan.nextLine();
            List<String> tokens2 = SPLITTER.splitToList(line2);

            similarUserId = tokens2.get(0);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }


    public void phaseOneModel() throws Exception {
        log.info(output.toString()+"/recommend.csv");
        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));
        Recommender recommender = new SlopeOneRecommender(model);
        Recommender cachingRecommender = new CachingRecommender(recommender);
        //recommend(user, num recomends)
        List<RecommendedItem> recommendations = cachingRecommender.recommend(Integer.valueOf(userId), 10);

        log.info("phaseOneModel recommendations "+recommendations.size());
        for (RecommendedItem recommendation : recommendations) {
            log.info(recommendation.toString());
        }

    }

    public void phaseTwoModel() throws Exception {

        RandomUtils.useTestSeed();

        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));

        RecommenderEvaluator evaluator =
                new AverageAbsoluteDifferenceRecommenderEvaluator();

        RecommenderBuilder builder = new RecommenderBuilder() {

            public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel model)
                    throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(1, similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood, similarity);

            }
        };
        double score = evaluator.evaluate(  builder, null, model, 0.7, 1.0);
        log.info("score " + score);


    }


    public void phaseThreeModel() throws Exception {



        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));

        UserSimilarity similarity = new LogLikelihoodSimilarity(model);

        UserNeighborhood neighborhood =  new NearestNUserNeighborhood(5, similarity, model);

        Recommender recommender = new GenericUserBasedRecommender(  model, neighborhood, similarity);

        List<RecommendedItem> recommendations =   recommender.recommend(Integer.valueOf(userId), Integer.valueOf(similarUserId));


        log.info("phaseThreeModel recommendations "+recommendations.size());

        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }

        log.info("userId  "+ userId);
        log.info("similarUserId  "+ similarUserId);

    }





}
