/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.ml.core.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ml.commons.domain.*;
import org.wso2.carbon.ml.commons.domain.config.SummaryStatisticsSettings;
import org.wso2.carbon.ml.core.exceptions.MLDataProcessingException;
import org.wso2.carbon.ml.core.exceptions.MLInputValidationException;
import org.wso2.carbon.ml.core.factories.DatasetProcessorFactory;
import org.wso2.carbon.ml.core.interfaces.DatasetProcessor;
import org.wso2.carbon.ml.core.interfaces.MLInputAdapter;
import org.wso2.carbon.ml.core.utils.BlockingExecutor;
import org.wso2.carbon.ml.core.utils.MLCoreServiceValueHolder;
import org.wso2.carbon.ml.core.utils.MLUtils;
import org.wso2.carbon.ml.core.utils.MLUtils.DataTypeFactory;
import org.wso2.carbon.ml.database.DatabaseService;
import org.wso2.carbon.ml.database.exceptions.DatabaseHandlerException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This object is responsible for reading a data-set using a {@link MLInputAdapter}, extracting meta-data, persist in ML
 * db and store the data stream in a user defined target path.
 */
public class MLDatasetProcessor {
    private static final Log log = LogFactory.getLog(MLDatasetProcessor.class);
    private SummaryStatisticsSettings summaryStatsSettings;
    private BlockingExecutor threadExecutor;
    private DatabaseService databaseService;
    private MemoryModelHandler memoryModelHandler = new MemoryModelHandler();

    public MLDatasetProcessor() {
        MLCoreServiceValueHolder valueHolder = MLCoreServiceValueHolder.getInstance();
        summaryStatsSettings = valueHolder.getSummaryStatSettings();
        databaseService = valueHolder.getDatabaseService();
        threadExecutor = valueHolder.getThreadExecutor();
    }

    public List<MLDatasetVersion> getAllDatasetVersions(int tenantId, String userName, long datasetId)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getAllVersionsetsOfDataset(tenantId, userName, datasetId);
            //return memoryModelHandler.getAllVersionsetsOfDataset(tenantId, userName, datasetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public MLDatasetVersion getVersionset(int tenantId, String userName, long versionsetId)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getVersionset(tenantId, userName, versionsetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }
    
    public SamplePoints getSamplePoints(int tenantId, String userName, long versionsetId)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getVersionsetSample(tenantId, userName, versionsetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public MLDatasetVersion getVersionSetWithVersion(int tenantId, String userName, long datasetId, String version)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getVersionSetWithVersion(datasetId, version, tenantId, userName);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<MLDataset> getAllDatasets(int tenantId, String userName) throws MLDataProcessingException {
        try {
//            MemoryModelHandler handler = new MemoryModelHandler();
//            handler.getAllDatasets();
            return memoryModelHandler.getAllDatasets(tenantId, userName);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public MLDataset getDataset(int tenantId, String userName, long datasetId) throws MLDataProcessingException {
        try {
            return memoryModelHandler.getDataset(tenantId, userName, datasetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<MLDatasetVersion> getAllVersionsetsOfDataset(int tenantId, String userName, long datasetId)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getAllVersionsetsOfDataset(tenantId, userName, datasetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<Object> getScatterPlotPoints(ScatterPlotPoints scatterPlotPoints) throws MLDataProcessingException {
        try {
            return memoryModelHandler.getScatterPlotPoints(scatterPlotPoints);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<Object> getScatterPlotPointsOfLatestVersion(long datasetId, ScatterPlotPoints scatterPlotPoints)
            throws MLDataProcessingException {
        try {
            List<MLDatasetVersion> versions = memoryModelHandler.getAllVersionsetsOfDataset(
                    scatterPlotPoints.getTenantId(), scatterPlotPoints.getUser(), datasetId);
            // Check whether versions are available for the dataset ID, if not it's not a valid ID
            if (versions.size() == 0) {
                throw new MLDataProcessingException(String.format("%s is not a valid dataset Id", datasetId));
            }
            long versionsetId = versions.get(versions.size() - 1).getId();
            scatterPlotPoints.setVersionsetId(versionsetId);
            return memoryModelHandler.getScatterPlotPoints(scatterPlotPoints);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<Object> getChartSamplePoints(int tenantId, String user, long versionsetId, String featureListString)
            throws MLDataProcessingException {
        try {
            return memoryModelHandler.getChartSamplePoints(tenantId, user, versionsetId, featureListString);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<Object> getChartSamplePointsOfLatestVersion(int tenantId, String user, long datasetId,
            String featureListString) throws MLDataProcessingException {
        try {
            List<MLDatasetVersion> versions = memoryModelHandler.getAllVersionsetsOfDataset(tenantId, user, datasetId);
            // Check whether versions are available for the dataset ID, if not it's not a valid ID
            if (versions.size() == 0) {
                throw new MLDataProcessingException(String.format("%s is not a valid dataset Id", datasetId));
            }
            long versionsetId = versions.get(versions.size() - 1).getId();
            return memoryModelHandler.getChartSamplePoints(tenantId, user, versionsetId, featureListString);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    /**
     * Process a given data-set; read the data-set as a stream, extract meta-data, persist the data-set in a target path
     * and persist meta-data in ML db.
     * 
     * @param dataset {@link org.wso2.carbon.ml.commons.domain.MLDataset} object
     * @throws MLInputValidationException
     */
    public void process(MLDataset dataset, InputStream inputStream) throws MLDataProcessingException,
            MLInputValidationException {
        dataset.setDataTargetType(MLCoreServiceValueHolder.getInstance().getDatasetStorage().getStorageType());
        DatasetProcessor datasetProcessor = DatasetProcessorFactory.buildDatasetProcessor(dataset, inputStream);
        datasetProcessor.process();
        String targetPath = datasetProcessor.getTargetPath();
        String firstLine = datasetProcessor.getFirstLine();
        CSVFormat dataFormat = DataTypeFactory.getCSVFormat(dataset.getDataType());
        String[] features = MLUtils.getFeatures(firstLine, dataFormat);
        List<Feature>set = new ArrayList<>();
        int j =1;
        for(String name: features){
            Feature fe = new Feature();
            fe.setName(name);
            fe.setIndex(j++);
            set.add(fe);
        }
        dataset.setDefaultFeatures(set);
        int featureSize = features.length;
        // persist dataset
        persistDataset(dataset);

        long datasetSchemaId = dataset.getId();

        List<String> featureNames = retreiveFeatureNames(datasetSchemaId);


        // If size is zero, then it is the first version of the dataset
        if (featureNames.size() != 0) {
            // Validate number of features
            if (featureSize != featureNames.size()) {
                String msg = String.format("Creating a dataset version failed because number of features[%s] in"
                        + " the dataset version does not match the number of features[%s] in the original"
                        + " dataset.", featureSize, featureNames.size());
                throw new MLDataProcessingException(msg);
            }

            if (dataset.isContainsHeader()) {
                // Validate feature names
                for (int i = 0; i < featureNames.size(); i++) {
                    String headerEntry = features[i];
                    if (!featureNames.get(i).equals(headerEntry)) {
                        String msg = String.format("Creating dataset version failed because Feature name: %s in"
                                + " the dataset version does not match the feature name: %s in the original"
                                + " dataset.", headerEntry, featureNames.get(i));
                        throw new MLDataProcessingException(msg);
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("datasetSchemaId: " + datasetSchemaId);
        }
        String versionsetName = dataset.getName() + "-" + dataset.getVersion();
        createDatasetArtifact(dataset.getId());


        // build the MLDatasetVersion
        MLDatasetVersion datasetVersion = MLUtils.getMLDatsetVersion(dataset.getTenantId(), datasetSchemaId,
                dataset.getUserName(), versionsetName, dataset.getVersion(), targetPath);

        createVersionArtifact(datasetVersion,dataset.getName());



        long datasetVersionId = retrieveDatasetVersionId(datasetVersion);
//        if (datasetVersionId != -1) {
//            // dataset version is already exist
//            throw new MLDataProcessingException(String.format(
//                    "Dataset already exists; data set [name] %s [version] %s", dataset.getName(), dataset.getVersion()));
//        }

        // Persist dataset version
        persistDatasetVersion(datasetVersion);
        datasetVersionId = retrieveDatasetVersionId(datasetVersion);

        if (log.isDebugEnabled()) {
            log.debug("datasetVersionId: " + datasetVersionId);
        }

        // start summary stats generation in a new thread, pass data set version id
        SummaryStatsGenerator task = new SummaryStatsGenerator(datasetSchemaId, datasetVersionId, summaryStatsSettings,
                datasetProcessor);
        threadExecutor.execute(task);
        threadExecutor.afterExecute(task, null);
        log.info(String.format("[Created] %s", dataset));

    }

    private List<String> retreiveFeatureNames(long datasetId) throws MLDataProcessingException {
        List<String> featureNames;
        try {
            featureNames = memoryModelHandler.getFeatureNames(datasetId);
            return featureNames;
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    private void persistDatasetVersion(MLDatasetVersion versionset) throws MLDataProcessingException {
        try {
            databaseService.insertDatasetVersion(versionset);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    private long retrieveDatasetVersionId(MLDatasetVersion versionset) {
        long datasetVersionId;
        try {
            datasetVersionId = memoryModelHandler.getVersionsetId(versionset.getName(), versionset.getTenantId(), versionset.getUserName());
            return datasetVersionId;
        } catch (DatabaseHandlerException e) {
            return -1;
        }
    }

    private void persistDataset(MLDataset dataset) throws MLDataProcessingException {
        try {
            MemoryModelHandler model = new MemoryModelHandler();
            List<MLDataset> datasets = model.addDatasets(dataset);
            String name = dataset.getName();
            int tenantId = dataset.getTenantId();
            String userName = dataset.getUserName();
            long datasetId = databaseService.getDatasetId(name, tenantId, userName);
            if (datasetId == -1) {
                databaseService.insertDatasetSchema(dataset);
                datasetId = databaseService.getDatasetId(name, tenantId, userName);
            }
            dataset.setId(datasetId);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public void deleteDataset(int tenantId, String userName, long datasetId) throws MLDataProcessingException {
        try {
            memoryModelHandler.deleteDataset(datasetId);
            log.info(String.format("[Deleted] [dataset] %s of [user] %s of [tenant] %s", datasetId, userName, tenantId));
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public void deleteDatasetVersion(int tenantId, String userName, long versionsetId) throws MLDataProcessingException {
        try {
            memoryModelHandler.deleteDatasetVersion(versionsetId);
            log.info(String.format("[Deleted] [dataset version] %s of [user] %s of [tenant] %s", versionsetId,
                    userName, tenantId));
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public List<String> getFeatureNames(long datasetId, String featureType) throws MLDataProcessingException {
        try {
            return memoryModelHandler.getFeatureNames(datasetId, featureType);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public String getSummaryStats(long datasetId, String featureName) throws MLDataProcessingException {
        try {
            return memoryModelHandler.getSummaryStats(datasetId, featureName);
        } catch (DatabaseHandlerException e) {
            throw new MLDataProcessingException(e.getMessage(), e);
        }
    }

    public void createDatasetArtifact(long id){

        MemoryModelHandler model = new MemoryModelHandler();
        ObjectMapper mapper = new ObjectMapper();
        MLDataset set = model.getDataset(id);
        MLDatasetArtifact artifact = model.genArtifact(set);


        File dir = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets" + File.separator + set.getName());
        if (!dir.exists()) {
            if (dir.mkdir()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }
        try {
            File file = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets" + File.separator + set.getName() + File.separator + "properties.json");
            if(!file.exists()) {
                mapper.writeValue(new File(
                        System.getProperty("carbon.home") + File.separator + "repository" +
                        File.separator + "deployment" + File.separator + "server" + File.separator +
                        "datasets" + File.separator +set.getName()+ File.separator + "properties.json"), artifact);
                String jsonInString = mapper.writeValueAsString(artifact);
                System.out.println(jsonInString);

                // Convert object to JSON string and print
                jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(artifact);
                System.out.println(jsonInString);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createVersionArtifact(MLDatasetVersion version,String datasetName){

        MemoryModelHandler model = new MemoryModelHandler();
        List<MLDataset> datasets = model.addVersions(version);
        ObjectMapper mapper = new ObjectMapper();
        List<MLDatasetVersion>versions = new ArrayList<>();
        MLDatasetVersion versionList = new MLDatasetVersion();

        for(int i=0; i<datasets.size(); i++){
            if(datasets.get(i).getId() == version.getDatasetId()){
                versions = datasets.get(i).getVersions();
                System.out.println("Size :"+datasets.get(i).getVersions().size());
                versionList = versions.get(versions.size()-1);
            }
        }
        try {
            mapper.writeValue( new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets" + File.separator +datasetName + File.separator + version.getVersion() + ".json"), versionList);
            String jsonInString = mapper.writeValueAsString(versionList);
            System.out.println(jsonInString);

            // Convert object to JSON string and print
            jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(versionList);
            System.out.println(jsonInString);

        } catch (IOException e) {
            e.printStackTrace();
        }
       }

}
