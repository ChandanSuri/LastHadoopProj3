package org.eduonix;

import com.google.common.collect.Lists;
import org.eduonix.etl.DuplicateStruct;
import org.eduonix.etl.EntityAnalysisETL;
import org.eduonix.etl.EntityStruct;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by ubu on 16.07.15.
 */
public class RunClusterFlow {

    private static final String projectRootPath = System.getProperty("user.dir");
    private static final String mapped_data = "mapped_data";
    private static final String clustered_data = "clustered";

    public static void main(String[] args) {

        Path inputFile = null;
        if(! AWSCloudProcessor.isAmazon) {
            inputFile = Paths.get(projectRootPath, mapped_data);
        } else {
            inputFile = Paths.get(projectRootPath, "output");
        }


        Path outputFile = Paths.get(projectRootPath, clustered_data);

        List<EntityStruct> entities = EntityAnalysisETL.extractData(inputFile);


        List<EntityStruct.DistanceStruct> clusters =  EntityAnalysisETL.transformData(entities);

        List<String> duplicates = Lists.newLinkedList();

        for (EntityStruct.DistanceStruct cluster : clusters) {

            if(cluster.duplicates.size() > 0 ) {


                for (DuplicateStruct duplicate : cluster.duplicates) {

                    String[] addressLine = duplicate.value.split(" ");

                    if (addressLine.length > 1 ) {
                        duplicates.add(duplicate.toString());
                    }
                }

            }
        }


        EntityAnalysisETL.loadData(duplicates, outputFile);

    }
}
