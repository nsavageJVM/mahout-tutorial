package org.eduonix.clustering;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.eduonix.datagenerator.StoreJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by ubu on 5/2/14.
 */
public class MahoutRecommenderDataGenerator {


    final static Logger log= LoggerFactory.getLogger(MahoutRecommenderDataGenerator.class);

    Path output = new Path("clustering/mockdata");

    public void setUpMockData(int records) throws Exception {

        /**
         * Setup configuration with prop.
         */
        Configuration conf =new Configuration();
        conf.setInt(StoreJob.props.store_records.name(), records);
        Job createInput= StoreJob.createJob(output, conf);
        createInput.waitForCompletion(true);
        FileSystem fs =  FileSystem.getLocal(new Configuration());

    }


    public void setUpPreferenceData() throws Exception {
        // this is a filter set where the filter is a list of products with their probability
        EnumSet<PreferenceValues> filterSet = EnumSet.allOf(PreferenceValues.class);

        // get the raw data to filter
        FileReader fileReader = new FileReader(new File("./clustering/mockdata/part-r-00000"));

        List<String[]> dataOutBuffer = Lists.newArrayList();
        BufferedReader br = new BufferedReader(fileReader);
        String csvLine = null;
        // if no more lines the readLine() returns null
        while ((csvLine = br.readLine()) != null) {
            Map<String, Integer> result = null;
            String[] lines = csvLine.split(",");
            String[] tmp = lines[1].split("_");
            String state = tmp[1];
            //last 1
            String product = lines[lines.length -1];
            // new code
            String[] tmp1 = lines[2].split("\t");
            String firstName = tmp1[1];
            String lastName = lines[3];
            String hashName = firstName+lastName;
            Integer nameId = Math.abs(hashName.hashCode());
            for(PreferenceValues filter : filterSet){

                if(filter.getPref(product, filter.name()) != null  ) {
                    result =filter.getPref(product, filter.name());

                    if (result.get("match").intValue() == 1) {
                        // match result data
                        String data = "found pref value: "+result.get("filterPrefValue")+" for filter name "+
                                filter.name()+ " with user hash value "+nameId+ " and productId "+result.get("filterId");
                        System.out.println(data);
                        String[] entries = {nameId.toString(), result.get("filterId").toString(),
                                result.get("filterPrefValue").toString(), ""+new Date().getTime()  };
                        dataOutBuffer.add(entries);
                    }
                }
            }

        }

        File file = new File("./clustering/mockdata/recommend.csv");
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for ( String[] sList: dataOutBuffer) {
            String csvOutLine = sList[0]+ '\t' +sList[1]+ '\t'  +sList[2]+ '\t' +sList[3]+ '\n';
            bw.write(csvOutLine);
        }
        bw.flush();
        fw.close();

        // now run recommendation code
        phaseOneModel(getUserIdFromTestData() );

    }

    private void phaseOneModel(String userIdForTest) throws Exception {
        log.info(output.toString()+"/recommend.csv");
        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));



        Recommender recommender = new SlopeOneRecommender(model);

        Recommender cachingRecommender = new CachingRecommender(recommender);
        //recommend(user, num recomends)
        List<RecommendedItem> recommendations = cachingRecommender.recommend(Integer.valueOf(userIdForTest), 10);

        log.info("recommendations "+recommendations.size());
        for (RecommendedItem recommendation : recommendations) {
            log.info(recommendation.toString());
        }

    }

    private String  getUserIdFromTestData() throws Exception {
        String userIdForTest = null;
        File file = new File(output.toString()+"/recommend.csv");
        try {
            Scanner scan = new Scanner(file);

            String line = scan.nextLine();
            String[] tokens = line.split("\t");
            userIdForTest = tokens[0];

            log.info(line);



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return userIdForTest;
    }




    public static enum PreferenceValues {

        No_Pref("0", "dog-food_10","choke-collar_15", "leather-collar_25","duck-caller_13"),
        Low_Pref("1", "cat-food_8","fuzzy-collar_19","salmon-bait_30", "antelope-caller_20"),
        Mid_Pref("2", "fish-food_20","turtle-pellets_5","seal-spray_25","salmon-bait_30", "snake-bite ointment_30"),
        Med_Pref( "3", "choke-collar_15", "antelope snacks_30", "hay-bail_5","cow-dung_2", "turtle-food_11"),
        High_Pref("4", "rodent-cage_40","antelope snacks_30","hay-bail_5","steel-leash_20","organic-dog-food_16" );

        Integer filterId = null;
        public String[] preferences;
        // constructor
        private PreferenceValues( String... preferences) {

            this.preferences = preferences;
        }

        public Map<String , Integer> getPref(String match, String filterName) {
            Integer pref = null;
            Pair<String,Integer> pair = null;
            Map<String, Integer> result = Maps.newHashMap();
            for(String s :preferences) {
                String[] tmp  = s.split("_");
                if (match.equals(tmp[0])) {
                    pref = Integer.parseInt(preferences[0]);
                    filterId = Math.abs(s.hashCode());
                    System.out.println("matched element "+match+" for filter "+filterName+", with preference = "+pref);
                    result.put("filterId", filterId);
                    result.put("filterPrefValue", pref);
                    result.put("match", 1);
                    break;
                } else {
                    result.put("match", Integer.valueOf(-1));

                }


            }
            return result;
        }
    }



}
