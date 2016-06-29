package org.wso2.carbon.ml.core.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.commons.domain.*;
import org.wso2.carbon.ml.commons.domain.config.MLConfiguration;
import org.wso2.carbon.ml.database.DatabaseService;
import org.wso2.carbon.ml.database.exceptions.DatabaseHandlerException;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by root on 5/6/16.
 */
public class MemoryModelHandler implements DatabaseService {

	public static List<MLDataset> datasets = new ArrayList<>();
	public static List<MLProject> projects = new ArrayList<>();

	public List<MLDataset> addDatasets(MLDataset dataset) {

		boolean exists = false;
		for (MLDataset set : datasets) {
			if (set.getName().equalsIgnoreCase(dataset.getName())) {
				exists = true;
			}
		}
		if (exists != true) {
			dataset.setId(datasets.size() + 1);
			//Adding id 1 for 0th index ;
			datasets.add(dataset);
		}

		return datasets;
	}

	public List<MLDataset> addVersions(MLDatasetVersion version) {

		int count = 0;
		for (int i = 0; i < datasets.size(); i++) {
			if (datasets.get(i).getId() == version.getDatasetId()) {
				if (!datasets.get(i).getVersions().contains(version.getName())) {
					version.setId(datasets.get(i).getVersions().size() + 1);
					datasets.get(i).getVersions().add(version);

				}
				List<MLDatasetVersion> ver = datasets.get(i).getVersions();
				for (int j = 0; j < datasets.get(i).getVersions().size(); j++) {
					System.out.println(
							"Id : " + ver.get(j).getId() + "Name : " + ver.get(j).getName() +
							"index when adding : " + j);
				}
			}
		}
		return datasets;
	}

	public List<MLProject> addProjects(MLProject project) {

		boolean exists = false;
		for (MLProject set : projects) {
			if (set.getName().equalsIgnoreCase(project.getName())) {
				exists = true;
			}
		}
		if (exists != true) {
			project.setId(projects.size() + 1);
			//Adding id 1 for 0th index ;
			projects.add(project);
		}
		return projects;
	}

	public MLAnalysis getAnalysis(long analysisId){
		MLAnalysis analysis = null;
		for(MLProject project: projects){
			for(MLAnalysis temp : project.getAnalyses()){
				if(temp.getId()==analysisId){
					analysis = temp;
				}
			}
		}
		return analysis;
	}

	public List<MLProject> addAnalysis(MLAnalysis analysis){

		long projectId = analysis.getProjectId();
		int analysisCount = 0;
		for(MLProject project : projects){
			int count = project.getAnalyses().size();
			analysisCount+=count;
		}

		for(int i = 0; i<projects.size(); i++) {
			if(projects.get(i).getId() == projectId){
				analysis.setId(analysisCount+1);
				projects.get(i).getAnalyses().add(analysis);
			}
		}
		return projects;
	}

	public void getAllDatasets() {
		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets");
		File[] folders = location.listFiles();
		List<MLDataset> datasetList = new ArrayList<>();

		if (folders.length != 0 && datasets.size() == 0) {

			for(File folder:folders){
				File[]files = folder.listFiles();
				for(File file : files){
					//To get the datasetNAme.json file as folder creates under dataset name
					String name = "properties.json";
					if(name.equalsIgnoreCase(file.getName())){
						try {
							MLDatasetArtifact artifact = mapper.readValue(file, MLDatasetArtifact.class);
							MLDataset dataset = genDataset(artifact);
							datasetList.add(dataset);

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			MLDataset[] datasetArr = new MLDataset[datasetList.size()];
			datasetList.toArray(datasetArr);
			//Sorts using insertion sort
			MLDataset[] arr = sortDatasets(datasetArr);

			for (MLDataset set : arr) {
				datasets.add(set);
			}
		getAllVersions();
		}

	}

	public MLDataset[] sortDatasets(MLDataset[] input) {
		MLDataset temp;
		for (int i = 1; i < input.length; i++) {
			for (int j = i; j > 0; j--) {
				if (input[j].getId() < input[j - 1].getId()) {
					temp = input[j];
					input[j] = input[j - 1];
					input[j - 1] = temp;
				}
			}
		}
		return input;

	}

	public MLModelData[] sortModelData(MLModelData[] input) {
		MLModelData temp;
		for (int i = 1; i < input.length; i++) {
			for (int j = i; j > 0; j--) {
				if (input[j].getId() < input[j - 1].getId()) {
					temp = input[j];
					input[j] = input[j - 1];
					input[j - 1] = temp;
				}
			}
		}
		return input;

	}

	public MLDatasetVersion[] sortVersions(MLDatasetVersion[] input) {
		MLDatasetVersion temp;
		for (int i = 1; i < input.length; i++) {
			for (int j = i; j > 0; j--) {
				if (input[j].getId() < input[j - 1].getId()) {
					temp = input[j];
					input[j] = input[j - 1];
					input[j - 1] = temp;
				}
			}
		}
		return input;

	}

	public void getAllVersions() {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "datasets");
		File[] list = location.listFiles();
		List<MLDatasetVersion> versionList;

		for (File file : list) {
			if (file.isDirectory()) {
				//Always assigning version list to new array since every iteration of list file array contains a different set of datasets
				versionList = new ArrayList<>();
				File[] files = file.listFiles();

				for (File f : files) {
					String fileName = "properties.json";
					//use to ignore dataset file as we cannot deserialize dataset file to a dataset version obj
					if (!fileName.equalsIgnoreCase(f.getName())) {
						try {
							MLDatasetVersion version = mapper.readValue(f, MLDatasetVersion.class);
							versionList.add(version);

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				MLDatasetVersion[] versions = new MLDatasetVersion[versionList.size()];
				versionList.toArray(versions);
				MLDatasetVersion[] arr = sortVersions(versions);

				for (MLDataset set : datasets) {
					//Version list always contains a list of versions for a single dataset as it goes through one folder at a time
					if (versionList.get(0).getDatasetId() == set.getId()) {
						for (MLDatasetVersion versionNew : arr) {
							//Adding dataset versions to relevant dataset
							set.getVersions().add(versionNew);
						}
					}
				}
			}
		}

	}

	public void getProjects() {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "projects");
		File[] files = location.listFiles();
		List<MLProject> projectList = new ArrayList<>();

		if (files.length != 0 && projects.size() == 0) {
			for (File f : files) {
				File[] proAr = f.listFiles();
				try {
					MLProject project = mapper.readValue(proAr[0], MLProject.class);
					projectList.add(project);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			MLProject[] projectArr = new MLProject[projectList.size()];
			projectList.toArray(projectArr);
			MLProject[] arr = sortProjects(projectArr);

			for (MLProject set : arr) {
				projects.add(set);
			}
			//getAnalyses();
			loadAnalyses();
		}

	}

	public MLProject[] sortProjects(MLProject[] input) {
		MLProject temp;
		for (int i = 1; i < input.length; i++) {
			for (int j = i; j > 0; j--) {
				if (input[j].getId() < input[j - 1].getId()) {
					temp = input[j];
					input[j] = input[j - 1];
					input[j - 1] = temp;
				}
			}
		}
		return input;

	}

	public void loadAnalyses(){

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "analyses");
		//All the files and folders in given location
		File[] list = location.listFiles();
		List<MLAnalysis> analysisList = new ArrayList<>();
		List<MLAnalysis> lists = new ArrayList<>();

		if (projects.get(0).getAnalyses().size()==0 && location.list().length != 0) {
			for (File file : list) {
				if (file.isDirectory()) {
					File[] files = file.listFiles();

					for (File f : files) {
						try {
							MLAnalysis analysis = mapper.readValue(f, MLAnalysis.class);
									analysisList.add(analysis);


						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			MLAnalysis[] analyses = new MLAnalysis[analysisList.size()];
			analysisList.toArray(analyses);
			MLAnalysis[] arr = sortAnalyses(analyses);
			lists = new ArrayList<MLAnalysis>(Arrays.asList(arr));


			for(MLAnalysis analysis : lists){
				for(MLProject project : projects){
					if(analysis.getProjectId()==project.getId()){
						project.getAnalyses().add(analysis);
					}
				}
			}
			getModelData();

		}

	}

	public MLAnalysis[] sortAnalyses(MLAnalysis[] input) {
		MLAnalysis temp;
		for (int i = 1; i < input.length; i++) {
			for (int j = i; j > 0; j--) {
				if (input[j].getId() < input[j - 1].getId()) {
					temp = input[j];
					input[j] = input[j - 1];
					input[j - 1] = temp;
				}
			}
		}
		return input;

	}

	public void addHyperParameters(List<MLHyperParameter> parameters, long analysisId){
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId()==analysisId){
					analysis.setHyperParameters(parameters);
				}
			}
		}
	}

	public void addModelConfigurations(List<MLModelConfiguration>config, long analysisId){
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId()==analysisId){
					analysis.setModelConfigurations(config);
				}
			}
		}

	}

	public void addCustomizedFeatures(List<MLCustomizedFeature> custFeatues, long analysisId){
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId()==analysisId){
					analysis.setFeatures(custFeatues);
				}
			}
		}
	}

	//adding model data to analysis bean
	public List<MLProject> addModel(MLModelData model){

		int modelCount = 0;
		for(MLProject project : projects){
			for(MLAnalysis analysis: project.getAnalyses()){
					int count = analysis.getModels().size();
					modelCount+=count;
			}
		}

		long analysisId = model.getAnalysisId();
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId) {
					model.setId(modelCount+1);
					analysis.getModels().add(model);
				}
			}
		}
		return projects;
	}

	//new method to change modelStatus
	public MLModelData changeStats(long modelId,String status){
		MLModelData model = null;

		for(MLProject project: projects){
			for(MLAnalysis analysis: project.getAnalyses()){
				for(MLModelData modelData : analysis.getModels()){
					if(modelData.getId() == modelId){
						modelData.setStatus(status);
						model = modelData;
					}
				}
			}
		}
		return model;
	}

	public void getModelData() {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "modeldata");
		File[] list = location.listFiles();
		List<MLModelData> modelList = new ArrayList<>();
		List<MLModelData>lists = new ArrayList<>();

		if (projects.get(0).getAnalyses().get(0).getModels().size() == 0 && location.list().length != 0) {
			for (File file : list) {
				if (file.isDirectory()) {
					File[] files = file.listFiles();

					for (File f : files) {
						try {
							MLModelData modeldata = mapper.readValue(f, MLModelData.class);
							modelList.add(modeldata);


						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			MLModelData[] models = new MLModelData[modelList.size()];
			modelList.toArray(models);
			MLModelData[] arr = sortModelData(models);
			lists = new ArrayList<MLModelData>(Arrays.asList(arr));

//			boolean contains = false;
//			for (MLModelData data : arr) {
//				for (MLModelData set : this.modelData) {
//					if (set.getId() == data.getId()) {
//						contains = true;
//					}
//				}
//				if (contains == false) {
//					this.modelData.add(data);
//				}
//			}
			for(MLModelData set: lists){

				for(MLProject project: projects){
					for(MLAnalysis analysis: project.getAnalyses()){
						if(analysis.getId()== set.getAnalysisId()){
							analysis.getModels().add(set);
						}
					}
				}
			}



		}

	}

	public MLModelData getModel(long modelId){
		MLModelData model = null;
		for(MLProject project: projects){
			for(MLAnalysis temp : project.getAnalyses()){
				for(MLModelData tempModel: temp.getModels()){
					if(tempModel.getId()==modelId){
						model = tempModel;
					}
				}
			}
		}
		return model;
	}

	public MLDatasetArtifact genArtifact(MLDataset dataset){

		MLDatasetArtifact artifact = new MLDatasetArtifact();
		artifact.setId(dataset.getId());
		artifact.setName(dataset.getName());
		artifact.setTenantId(dataset.getTenantId());
		artifact.setUserName(dataset.getUserName());
		artifact.setDefaultFeatures(dataset.getDefaultFeatures());
		artifact.setSourcePath(dataset.getSourcePath());
		artifact.setDataType(dataset.getDataType());
		artifact.setComments(dataset.getComments());
		artifact.setContainsHeader(dataset.isContainsHeader());
		return artifact;
	}

	public MLDataset getDataset(long datasetId){
		MLDataset dataset = null;
		for(MLDataset set: datasets){
			if(set.getId()== datasetId){
				dataset = set;
			}
		}
		return dataset;
	}

	//Use to convert MLDatasetArtifact object to a MLDataset object during server startup
	public MLDataset genDataset(MLDatasetArtifact artifact){

		MLDataset dataset = new MLDataset();
		dataset.setId(artifact.getId());
		dataset.setName(artifact.getName());
		dataset.setTenantId(artifact.getTenantId());
		dataset.setUserName(artifact.getUserName());
		dataset.setDefaultFeatures(artifact.getDefaultFeatures());
		dataset.setSourcePath(artifact.getSourcePath());
		dataset.setDataType(artifact.getDataType());
		dataset.setComments(artifact.getComments());
		dataset.setContainsHeader(artifact.isContainsHeader());
		return dataset;
	}

	public MLDatasetVersion getVersion(long datasetId, long versionId){
		MLDatasetVersion version = new MLDatasetVersion();

		for(MLDataset dataset: datasets){
			if(dataset.getId() == datasetId){
			for(MLDatasetVersion ver: dataset.getVersions()){
				if(ver.getId() == versionId){
					version = ver;
				}
				}
			}
		}
	return version;
	}

	/**
	 * Methods related to MLDatasetProcessor
	 */

	public List<MLDatasetVersion> getAllVersionsetsOfDataset(int tenantId, String userName, long datasetId) throws DatabaseHandlerException {
		List<MLDatasetVersion>versions = new ArrayList<>();
		for(MLDataset dataset: datasets){
			if(dataset.getId()==datasetId && dataset.getUserName().equalsIgnoreCase(userName)&& dataset.getTenantId()==tenantId){
				versions = dataset.getVersions();
			}
		}
		return versions;
	}

	public MLDatasetVersion getVersionset(int tenantId, String userName, long versionsetId) throws DatabaseHandlerException {
		MLDatasetVersion datasetVersion = new MLDatasetVersion();
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion version : dataset.getVersions()){
				if(version.getId() == versionsetId && version.getTenantId()==tenantId && version.getUserName().equalsIgnoreCase(userName)){
					datasetVersion = version;
				}
			}
		}
		return datasetVersion;
	}

	public MLDatasetVersion getVersionSetWithVersion(long datasetId, String version, int tenantId, String userName) throws DatabaseHandlerException {
		MLDatasetVersion datasetVersion = new MLDatasetVersion();
		for(MLDataset dataset: datasets){
			for(MLDatasetVersion ver : dataset.getVersions()){
				if(ver.getDatasetId()==datasetId && ver.getName().equalsIgnoreCase(version) && ver.getTenantId()==tenantId && ver.getUserName().equalsIgnoreCase(userName)){
					datasetVersion = ver;
				}
			}
		}
		return datasetVersion;
	}

	public List<MLDataset> getAllDatasets(int tenantId, String userName) throws DatabaseHandlerException {
		List<MLDataset> datasetList = new ArrayList<>();
		for(MLDataset dataset : datasets){
			if(dataset.getTenantId() == tenantId && dataset.getUserName().equalsIgnoreCase(userName)){
				datasetList.add(dataset);
			}
		}
		return datasetList;
	}

	public MLDataset getDataset(int tenantId, String userName, long datasetId) throws DatabaseHandlerException {

		MLDataset dataset = new MLDataset();
		for(MLDataset set : datasets){
			if(set.getId() == datasetId && set.getTenantId()==tenantId && set.getUserName().equalsIgnoreCase(userName)){
				dataset = set;
			}
		}
		return dataset;
	}

	public List<String> getFeatureNames(long datasetId) throws DatabaseHandlerException {

		List<String>names = new ArrayList<>();
		for(MLDataset dataset: datasets){
			if(dataset.getId()==datasetId){
				List<Feature>features = dataset.getDefaultFeatures();
				for(Feature feature: features){
					names.add(feature.getName());
				}
			}
		}
		return names;
	}

	public long getVersionsetId(String datasetVersionName, int tenantId, String userName) throws DatabaseHandlerException {
		long versionId = 0;
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion version : dataset.getVersions()){
				if(version.getName().equalsIgnoreCase(datasetVersionName) && version.getTenantId() == tenantId && version.getUserName().equalsIgnoreCase(userName)){
					versionId = version.getId();
				}
			}
		}
		return versionId;
	}

	public long getDatasetId(String name,int tenantId, String username) throws DatabaseHandlerException {
		long datasetId = 0;
		for(MLDataset dataset: datasets){
			if(dataset.getName().equalsIgnoreCase(name)){
				datasetId = dataset.getId();
			}
		}
		return datasetId;
	}

	public void deleteDataset(long datasetId) throws DatabaseHandlerException  {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "datasets");
		File[] folders = location.listFiles();

		if (folders.length != 0) {

			for (File folder : folders) {
				File[] files = folder.listFiles();
				for (File file : files) {
					//To get the datasetNAme.json file as folder creates under dataset name
					String name = "properties.json";
					if (name.equalsIgnoreCase(file.getName())) {
						try {
							MLDatasetArtifact artifact = mapper.readValue(file, MLDatasetArtifact.class);
							if (artifact.getId() == datasetId) {
								folder.delete();
								System.out.println(file.getName() + " is deleted!");

							}

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		for(MLDataset set : datasets){
			if(set.getId() == datasetId){
				datasets.remove(set);
			}
		}

	}

	public void deleteDatasetVersion(long versionsetId) throws DatabaseHandlerException {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "datasets");
		File[] list = location.listFiles();

		for (File file : list) {
			if (file.isDirectory()) {
				File[] files = file.listFiles();

				for (File f : files) {
					String fileName = "properties.json";
					//use to ignore dataset file as we cannot deserialize dataset file to a dataset version obj
					if (!fileName.equalsIgnoreCase(f.getName())) {
						try {
							MLDatasetVersion version = mapper.readValue(f, MLDatasetVersion.class);
							if(version.getId()==versionsetId){
								file.delete();
								System.out.println(file.getName() + " is deleted!");
							}

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}}}

	}

	public List<String> getFeatureNames(long datasetId, String featureType) throws DatabaseHandlerException {
		List<String> features = new ArrayList<>();
		for(MLDataset dataset : datasets){
			if(dataset.getId() == datasetId){
				for(Feature feature : dataset.getDefaultFeatures()){
					if(feature.getType().equalsIgnoreCase(featureType)){
						features.add(feature.getName());
					}
				}
			}
		}
		return features;
	}

	public String getSummaryStats(long datasetId, String featureName) throws DatabaseHandlerException {
		String stats = " ";
		for(MLDataset dataset : datasets){
			if(dataset.getId() == datasetId){
				MLDatasetVersion latestVersion = dataset.getVersions().get(dataset.getVersions().size()-1);
				List<FeatureSummary>featureSum = latestVersion.getFeatureSum();
				for(FeatureSummary summary : featureSum){
					if(summary.getFieldName().equalsIgnoreCase(featureName)){
						stats = summary.getSummaryStats();
					}
				}
			}
		}
		return stats;
	}

	@Override public String getDatasetVersionUri(long datasetVersionId) throws DatabaseHandlerException {
		//location
		return null;
	}

	@Override public String getDatasetUri(long datasetId) throws DatabaseHandlerException {
		//location
		return null;
	}

	@Override public List<Object> getScatterPlotPoints(ScatterPlotPoints scatterPlotPoints) throws DatabaseHandlerException {
		// Get the sample from the database.
		SamplePoints sample = getVersionsetSample(scatterPlotPoints.getTenantId(),scatterPlotPoints.getUser(), scatterPlotPoints.getVersionsetId());
		List<Object> points = new ArrayList<Object>();

		// Converts the sample to a JSON array.
		List<List<String>> columnData = sample.getSamplePoints();
		Map<String, Integer> dataHeaders = sample.getHeader();

		int firstFeatureColumn = dataHeaders.get(scatterPlotPoints.getxAxisFeature());
		int secondFeatureColumn = dataHeaders.get(scatterPlotPoints.getyAxisFeature());
		int thirdFeatureColumn = dataHeaders.get(scatterPlotPoints.getGroupByFeature());
		for (int row = 0; row < columnData.get(thirdFeatureColumn).size(); row++) {
			if (columnData.get(firstFeatureColumn).get(row) != null
			    && columnData.get(secondFeatureColumn).get(row) != null
			    && columnData.get(thirdFeatureColumn).get(row) != null
			    && !columnData.get(firstFeatureColumn).get(row).isEmpty()
			    && !columnData.get(secondFeatureColumn).get(row).isEmpty()
			    && !columnData.get(thirdFeatureColumn).get(row).isEmpty()) {
				Map<Double, Object> map1 = new HashMap<Double, Object>();
				Map<Double, Object> map2 = new HashMap<Double, Object>();
				String val1 = columnData.get(secondFeatureColumn).get(row);
				String val2 = columnData.get(firstFeatureColumn).get(row);
				if (NumberUtils.isNumber(val1) && NumberUtils.isNumber(val2)) {
					map2.put(Double.parseDouble(val1), columnData.get(thirdFeatureColumn).get(row));
					map1.put(Double.parseDouble(val2), map2);
					points.add(map1);
				}
			}
		}

		return points;
	}

	@Override public List<Object> getChartSamplePoints(int tenantId, String user, long versionsetId, String featureListString) throws DatabaseHandlerException {
		List<Object> points = new ArrayList<Object>();

		// Get the sample from the database.
		SamplePoints sample = getVersionsetSample(tenantId, user, versionsetId);

		if (sample == null) {
			return points;
		}
		// Converts the sample to a JSON array.
		List<List<String>> columnData = sample.getSamplePoints();
		Map<String, Integer> dataHeaders = sample.getHeader();

		if (featureListString == null || featureListString.isEmpty()) {
			return points;
		}

		// split categoricalFeatureListString String into a String array
		String[] featureList = featureListString.split(",");

		// Check whether features exists
		for (String feature: featureList) {
			if (!dataHeaders.containsKey(feature)) {
				throw new DatabaseHandlerException(String.format("%s is not a feature of version set Id: %s",
				                                                 feature, versionsetId));
			}
		}

		// for each row in a selected categorical feature, iterate through all features
		for (int row = 0; row < columnData.get(dataHeaders.get(featureList[0])).size(); row++) {

			Map<String, Object> data = new HashMap<String, Object>();

			// for each categorical feature in same row put value into a point(JSONObject)
			// {"Soil_Type1":"0","Soil_Type11":"0","Soil_Type10":"0","Cover_Type":"4"}
			for (int featureCount = 0; featureCount < featureList.length; featureCount++) {
				data.put(featureList[featureCount], columnData.get(dataHeaders.get(featureList[featureCount]))
				                                              .get(row));
			}

			points.add(data);
		}
		return points;
	}


	/**
	 * Methods related to MLProjectHandler
	 */

	public void deleteProject(int tenantId, String userName, long projectId) throws DatabaseHandlerException {

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "projects");
		File[] folders = location.listFiles();

		if (folders.length != 0) {

			for (File folder : folders) {
				File[] files = folder.listFiles();
				for (File file : files) {
						try {
							MLProject artifact = mapper.readValue(file, MLProject.class);
							if (artifact.getId() == projectId &&
							    artifact.getUserName().equalsIgnoreCase(userName) &&
							    artifact.getTenantId() == tenantId) {
								folder.delete();
								System.out.println(file.getName() + " is deleted!");

							}

						} catch (IOException e) {
							e.printStackTrace();
						}
				}
			}
		}

		for(MLProject project: projects){
			if(project.getId() == projectId){
				projects.remove(project);
			}
		}

	}

	public MLProject getProject(int tenantId, String userName, String projectName) throws DatabaseHandlerException {
		MLProject  mlproject = new MLProject();
		for(MLProject project: projects){
			if(project.getName().equalsIgnoreCase(projectName) && project.getTenantId() == tenantId && project.getUserName().equalsIgnoreCase(userName)){
				mlproject = project;
			}
		}
		return mlproject;
	}

	public MLProject getProject(int tenantId, String userName, long projectId) throws DatabaseHandlerException {
		MLProject project = new MLProject();
		for(MLProject pro : projects){
			if(pro.getId()==projectId && pro.getTenantId() == tenantId && pro.getUserName().equalsIgnoreCase(userName)){
				project = pro;
			}
		}
		return project;
	}

	public List<MLProject> getAllProjects(int tenantId, String userName) throws DatabaseHandlerException {
		List<MLProject> projects = new ArrayList<>();
		for(MLProject project : this.projects){
			if(project.getTenantId() == tenantId && project.getUserName().equalsIgnoreCase(userName)){
				projects.add(project);
			}
		}
		return projects;
	}

	public List<MLModelData> getProjectModels(int tenantId, String userName, long projectId) throws DatabaseHandlerException  {
		List<MLModelData> models= new ArrayList<>();
		for(MLProject project : projects){
			if(project.getId() == projectId && project.getTenantId() == tenantId && project.getUserName().equalsIgnoreCase(userName)){
				for(MLAnalysis analysis : project.getAnalyses()) {
					for(MLModelData model :  analysis.getModels()){
						models.add(model);
					}
				}
			}
		}
		return models;
	}

	public List<MLAnalysis> getAllAnalysesOfProject(int tenantId, String userName, long projectId)  throws DatabaseHandlerException {
		List<MLAnalysis> analyses = new ArrayList<>();
		for(MLProject project : projects){
			if(project.getId() == projectId && project.getTenantId() == tenantId && project.getUserName().equalsIgnoreCase(userName)){
				analyses = project.getAnalyses();
			}
		}
		return analyses;
	}

	public MLAnalysis getAnalysisOfProject(int tenantId, String userName, long projectId, String analysisName) throws DatabaseHandlerException {

		MLAnalysis analysis = new MLAnalysis();
		for(MLProject project : projects) {
			if(project.getId() == projectId && project.getUserName().equalsIgnoreCase(userName) && project.getTenantId() == tenantId){
				for(MLAnalysis mlAnalysis : project.getAnalyses()){
					if (mlAnalysis.getName().equalsIgnoreCase(analysisName)){
						analysis = mlAnalysis;
					}
				}
			}
		}
		return analysis;
	}

	/**
	 * Methods related to MLAnalysisHandler
	 */

	public void insertFeatureCustomized(long analysisId, List<MLCustomizedFeature> customizedFeatures, int tenantId, String userName) throws DatabaseHandlerException {

		for(MLProject project : projects){

			for(MLAnalysis analysis : project.getAnalyses()) {
				if(analysis.getId() == analysisId && analysis.getTenantId() == tenantId && analysis.getUserName().equalsIgnoreCase(userName)){
					analysis.setFeatures(customizedFeatures);
				}
			}
		}
	}

	public void insertDefaultsIntoFeatureCustomized(long analysisId, MLCustomizedFeature customizedValues) throws DatabaseHandlerException {

		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()) {
				if(analysis.getId() == analysisId){
					analysis.getFeatures().add(customizedValues);
				}
			}
		}
	}

	public List<FeatureSummary> getFeatures(int tenantId, String userName, long analysisId, int limit, int offset) throws DatabaseHandlerException {
		List<FeatureSummary> summary = new ArrayList<>();
		long datasetId = 0;

		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId && analysis.getTenantId() == tenantId && analysis.getUserName().equalsIgnoreCase(userName)){
					datasetId = project.getDatasetId();
				}
			}
		}

		for(MLDataset dataset : datasets){
			if(dataset.getId() == datasetId){
				List<MLDatasetVersion> versions = dataset.getVersions();
				MLDatasetVersion version = versions.get(versions.size()-1);
				summary = version.getFeatureSum();
			}
		}
	return summary;
	}

	public List<MLCustomizedFeature> getCustomizedFeatures(int tenantId, String userName, long analysisId, int limit, int offset){

		List<MLCustomizedFeature> features = new ArrayList<>();
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId && analysis.getTenantId() == tenantId && analysis.getUserName().equalsIgnoreCase(userName)){
					features = analysis.getFeatures();
				}
			}
		}
		return features;
	}

	public List<String> getFeatureNames(String analysisId, String featureType) throws DatabaseHandlerException {
		List<String>featureNames= new ArrayList<>();
		long id = Long.parseLong(analysisId);
		for(MLProject project : projects) {
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == id){
					for(MLCustomizedFeature feature : analysis.getFeatures()){
						if(feature.getType().equalsIgnoreCase(featureType)){
							featureNames.add(feature.getName());
						}
					}
				}
			}
		}
		return featureNames;
	}



	//remove these insert methods from interface as insertions happen in a different place
	@Override public MLConfiguration getMlConfiguration() {
		return null;
	}

	@Override public void insertDatasetSchema(MLDataset dataset) throws DatabaseHandlerException {
	//Remove
	}

	@Override public void insertDatasetVersion(MLDatasetVersion datasetVersion) throws DatabaseHandlerException {
	//remove
	}

	@Override public void insertProject(MLProject project) throws DatabaseHandlerException {
	//remove
	}

	@Override public void insertAnalysis(MLAnalysis analysis) throws DatabaseHandlerException {
	//remove
	}

	@Override public void insertModel(MLModelData model) throws DatabaseHandlerException {
	//remove
	}



	@Override public List<String> getFeatureNames(String analysisId) throws DatabaseHandlerException {
		List<String>featureNames = new ArrayList<>();
		long id = Long.parseLong(analysisId);
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == id){
					for(MLCustomizedFeature feature : analysis.getFeatures()){
						featureNames.add(feature.getName());
					}
				}
			}
		}
		return featureNames;
	}

	@Override public String getSummaryStats(int tenantId, String user, long analysisId, String featureName) throws DatabaseHandlerException {
		String summaryStats = "";
		long datasetId = 0;
		long projectId =0;
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId && analysis.getUserName().equalsIgnoreCase(user) && analysis.getTenantId() == tenantId){
					projectId = project.getId();
				}
			}
		}
		for(MLProject project : projects){
			if(project.getId() == projectId){
				datasetId = project.getDatasetId();
			}
		}

		for(MLDataset dataset : datasets){
			if(dataset.getId() == datasetId){
				MLDatasetVersion latestVersion = dataset.getVersions().get(dataset.getVersions().size() -1);
				List<FeatureSummary>feature = latestVersion.getFeatureSum();
				for(FeatureSummary sum : feature){
					if(sum.getFieldName().equalsIgnoreCase(featureName)){
						summaryStats = sum.getSummaryStats();
					}
				}
			}
		}
		return summaryStats;
	}

	@Override public SamplePoints getVersionsetSample(int tenantId, String user, long versionsetId) throws DatabaseHandlerException {
		return null;
		//Use databse to store these
	}

	@Override public int getFeatureCount(long datasetVersionId) throws DatabaseHandlerException {
		int count  = 0;
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion version : dataset.getVersions()){
				if(version.getId() == datasetVersionId){
					count =version.getFeatureSum().size();
				}
			}
		}
		return count;
	}

	@Override public void updateSummaryStatistics(long datasetSchemaId, long datasetVersionId, SummaryStats summaryStats) throws DatabaseHandlerException {
		MLDataset dataset = new MLDataset();
		MLDatasetVersion version = new MLDatasetVersion();
		for(MLDataset data : datasets){
			if(data.getId() == datasetSchemaId){
				dataset = data;
				for(MLDatasetVersion ver : data.getVersions()){
					if(ver.getId() == datasetVersionId){
						version = ver;
					}
				}
			}
		}
		int count = getFeatureCount(datasetSchemaId);
		JSONArray summaryStatJson;
		String columnName;
		int index;
		for (Map.Entry<String, Integer> mapping : summaryStats.getHeaderMap().entrySet()) {
			index = mapping.getValue();
			columnName = mapping.getKey();
			// Get the JSON representation of the column summary.
			try {
				summaryStatJson = createJson(summaryStats.getType()[index],
				                             summaryStats.getGraphFrequencies().get(index),
				                             summaryStats.getMissing()[index], summaryStats.getUnique()[index],
				                             summaryStats.getDescriptiveStats().get(index));
				if(count == 0){
					Feature feature  = new Feature();
					feature.setName(mapping.getKey());
					feature.setType(summaryStats.getType()[index]);
					feature.setIndex(index);
				}
				List<Feature> features = dataset.getDefaultFeatures();
				for(Feature fe: features){
					if(fe.getName().equalsIgnoreCase(columnName)){
						FeatureSummary sumObj = new FeatureSummary(fe.getName(),fe.isInclude(),fe.getType(),fe.getImputeOption(),summaryStatJson.toString(),fe.getIndex());
						version.getFeatureSum().add(sumObj);
					}

				}


			}catch(Exception e){

			}

		}
	}

	@Override public void updateSamplePoints(long datasetVersionId, SamplePoints samplePoints)
			throws DatabaseHandlerException {

	}

	@Override public void insertModelConfigurations(long analysisId, List<MLModelConfiguration> modelConfigs) throws DatabaseHandlerException {
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId){
					analysis.setModelConfigurations(modelConfigs);
				}
			}
		}

	}

	@Override public void insertHyperParameters(long analysisId, List<MLHyperParameter> hyperParameters, String algorithmName) throws DatabaseHandlerException {
		for(MLProject project: projects){
			for(MLAnalysis analysis: project.getAnalyses()){
				if(analysis.getId() == analysisId){
					for(MLModelConfiguration config : analysis.getModelConfigurations()){
						if(config.getKey().equalsIgnoreCase("algorithmName")){
							analysis.setHyperParameters(hyperParameters);
						}
					}
				}
			}
		}
	}

	@Override public void updateModelSummary(long modelId, ModelSummary modelSummary) throws DatabaseHandlerException {
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model: analysis.getModels()){
					if(model.getId() == modelId){
						model.setModelSummary(modelSummary);
					}
				}
			}
		}
	}

	@Override public void updateModelStorage(long modelId, String storageType, String location) throws DatabaseHandlerException {
		for(MLProject project : projects) {
			for(MLAnalysis  analysis: project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getId() == modelId){
						model.setStorageType(storageType);
						model.setStorageDirectory(location);
					}
				}
			}
		}
	}

	@Override public boolean isValidModelId(int tenantId, String userName, long modelId) throws DatabaseHandlerException {
		boolean isValid = false;
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData modeldata : analysis.getModels()){
					if(modeldata.getId() == modelId && modeldata.getTenantId() == tenantId && modeldata.getUserName().equalsIgnoreCase(userName) ){
						if(modeldata.getName()!=null){
							isValid = true;
							}else {
							isValid = false;
						}
					}
				}
			}
		}
		return isValid;
	}

	@Override public boolean isValidModelStatus(long modelId, int tenantId, String userName) throws DatabaseHandlerException {
		boolean isValid = false;
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getId() == modelId && model.getTenantId() == tenantId && model.getUserName().equalsIgnoreCase(userName)){
						if(model.getStatus().equalsIgnoreCase(MLConstants.MODEL_STATUS_COMPLETE)){
							isValid =true;
						}else{
							isValid=false;
						}
					}
				}
			}
		}
		return isValid;
	}

	@Override public long getDatasetVersionIdOfModel(long modelId) throws DatabaseHandlerException {
		long version = 0;
		String versionSet = "";
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getId() == modelId){
						versionSet = model.getDatasetVersion();
					}
				}
			}
		}
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion ver : dataset.getVersions()){
				if(ver.getName().equalsIgnoreCase(versionSet)){
					version = ver.getId();
				}
			}
		}
		return version;
	}

	@Override public long getDatasetId(long datasetVersionId) throws DatabaseHandlerException {
		long datasetId = 0;
		for(MLDataset dataset: datasets){
			for(MLDatasetVersion version : dataset.getVersions()) {
				if(version.getId() == datasetVersionId){
					datasetId = version.getDatasetId();
				}
			}
		}
		return datasetId;
	}

	@Override public String getDataTypeOfModel(long modelId) throws DatabaseHandlerException {
		String type = "";
		String version = "";
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()) {
				for(MLModelData model : analysis.getModels()){
					if(model.getId() == modelId){
						version = model.getDatasetVersion();
					}
				}
			}
		}
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion ver : dataset.getVersions()){
				if(ver.getName().equalsIgnoreCase(version));
				type = dataset.getDataType();
			}
		}
		return type;
	}

	@Override public String getAStringModelConfiguration(long analysisId, String configKey) throws DatabaseHandlerException {
		String config= "";
		for(MLProject project : projects){
			for(MLAnalysis analysis: project.getAnalyses()){
				if(analysis.getId() == analysisId){
					for(MLModelConfiguration con : analysis.getModelConfigurations()){
						if(con.getKey().equalsIgnoreCase(configKey)){
							config = con.getValue();
						}
					}
				}
			}
		}
		return config;
	}

	@Override public double getADoubleModelConfiguration(long analysisId, String configKey) throws DatabaseHandlerException {
		double config =0;
		for(MLProject project : projects){
			for(MLAnalysis analysis  : project.getAnalyses()){
				if(analysis.getId() == analysisId) {
					for (MLModelConfiguration conf : analysis.getModelConfigurations()) {
						if (conf.getKey().equalsIgnoreCase(configKey)) {
							config = Double.parseDouble(conf.getValue());
						}
					}
				}
			}
		}
		return config;
	}

	@Override public boolean getABooleanModelConfiguration(long analysisId, String configKey) throws DatabaseHandlerException {

		boolean config = false;
		for(MLProject project : projects) {
			for(MLAnalysis analysis: project.getAnalyses()){
				if(analysis.getId() == analysisId){
					for(MLModelConfiguration conf : analysis.getModelConfigurations()){
						if(conf.getKey().equalsIgnoreCase(configKey)){
							config = Boolean.parseBoolean(conf.getValue());
						}
					}
				}
			}
		}
		return config;
	}

	@Override public List<MLHyperParameter> getHyperParametersOfModel(long analysisId, String algorithmName) throws DatabaseHandlerException {
		List<MLHyperParameter>params = new ArrayList<>();
		for(MLProject project : projects){
			for(MLAnalysis analysis:project.getAnalyses()){
				for(MLModelConfiguration config : analysis.getModelConfigurations()){
					if(config.getKey().equalsIgnoreCase("algorithmName")){
						params = analysis.getHyperParameters();
					}
				}
			}
		}
		return params;
	}

	//Find what are values that should be added as key and value
	@Override public Map<String, String> getHyperParametersOfModelAsMap(long modelId) throws DatabaseHandlerException {

		List<MLHyperParameter> params = new ArrayList<>();
		Map<String, String> paramMap = new HashMap<>();
		for(MLProject project : projects){
			for(MLAnalysis analysis: project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getId() == modelId){
						params = analysis.getHyperParameters();
					}
				}
			}
		}
		for (MLHyperParameter i : params) paramMap.put(i.getKey(),i.getValue());
		return paramMap;
	}

	@Override public Workflow getWorkflow(long analysisId) throws DatabaseHandlerException {
		//No workflow in current structure
		return null;
	}

	//Check this again
	@Override public MLStorage getModelStorage(long modelId) throws DatabaseHandlerException {
		MLStorage storage = new MLStorage();
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData data: analysis.getModels()){
					if(data.getId() == modelId){
						storage.setType(data.getStorageType());
						storage.setLocation(data.getStorageDirectory());
					}
				}
			}
		}
		return storage;
	}

	@Override public List<MLAnalysis> getAllAnalyses(int tenantId, String userName) throws DatabaseHandlerException {
		List<MLAnalysis>analyses = new ArrayList<>();
		for(MLProject project: projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getName().equalsIgnoreCase(userName)&& analysis.getTenantId()==tenantId){
					analyses.add(analysis);
				}
			}
		}
		return analyses;
	}

	@Override public MLModelData getModel(int tenantId, String userName, String modelName) throws DatabaseHandlerException {
		MLModelData modelData = new MLModelData();
		for (MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getName().equalsIgnoreCase(modelName) && model.getTenantId() == tenantId && model.getUserName().equalsIgnoreCase(userName)){
						modelData = model;
					}
				}
			}
		}
		return modelData;
	}

	@Override public MLModelData getModel(int tenantId, String userName, long modelId) throws DatabaseHandlerException {
		MLModelData modelData = new MLModelData();
		for (MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getId()==modelId && model.getTenantId() == tenantId && model.getUserName().equalsIgnoreCase(userName)){
						modelData = model;
					}
				}
			}
		}
		return modelData;
	}

	@Override public List<MLModelData> getAllModels(int tenantId, String userName) throws DatabaseHandlerException {
		List<MLModelData> models = new ArrayList<>();
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model : analysis.getModels()){
					if(model.getTenantId()==tenantId && model.getUserName().equalsIgnoreCase(userName)){
						models.add(model);
					}
				}
			}
		}
		return models;
	}

	@Override public long getDatasetSchemaIdFromAnalysisId(long analysisId) throws DatabaseHandlerException {
		long datasetId = 0;

		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				if(analysis.getId() == analysisId){
					datasetId = project.getDatasetId();

				}
			}
		}
		return datasetId;
	}

	//Find from which places model should be deleted
	@Override public void deleteModel(int tenantId, String userName, long modelId)
			throws DatabaseHandlerException {

	}

	@Override public List<MLModelData> getAllModels(int tenantId, String userName, long analysisId) throws DatabaseHandlerException {
		List<MLModelData> models = new ArrayList<>();
		for(MLProject project: projects){
			for(MLAnalysis analysis:project.getAnalyses()){
				if(analysis.getId()==analysisId && analysis.getUserName().equalsIgnoreCase(userName)&&analysis.getTenantId()==tenantId){
					for(MLModelData model:analysis.getModels()){
						models.add(model);
					}
				}
			}
		}return models;
	}

	@Override public ModelSummary getModelSummary(long modelId) throws DatabaseHandlerException {
    ModelSummary summary = null;
		for(MLProject project: projects){
			for(MLAnalysis analysis:project.getAnalyses()){
				for(MLModelData model:analysis.getModels()){
					if(model.getId() == modelId){
						summary = model.getModelSummary();
					}

				}
			}
		}
		return summary;
	}

	@Override public void updateModelStatus(long modelId, String status) throws DatabaseHandlerException {
		for(MLProject project: projects){
			for(MLAnalysis analysis:project.getAnalyses()){
					for(MLModelData model:analysis.getModels()){
						if(model.getId() == modelId){
							model.setStatus(status);
					}
				}
			}
		}
	}

	@Override public MLAnalysis getAnalysis(int tenantId, String userName, long analysisId) throws DatabaseHandlerException {
		MLAnalysis analysis = null;
		for(MLProject project: projects){
			for(MLAnalysis temp : project.getAnalyses()){
				if(temp.getId()==analysisId && temp.getTenantId()==tenantId && temp.getUserName().equalsIgnoreCase(userName)){
					analysis = temp;
				}
			}
		}
		return analysis;
	}


	@Override public void deleteAnalysis(int tenantId, String userName, long analysisId) throws DatabaseHandlerException {
		ObjectMapper mapper = new ObjectMapper();
		File location = new File(
				System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
				"deployment" + File.separator + "server" + File.separator + "analyses");
		File[] folders = location.listFiles();

		if (folders.length != 0) {

			for (File folder : folders) {
				File[] files = folder.listFiles();
				for (File file : files) {
					try {
						MLAnalysis artifact = mapper.readValue(file, MLAnalysis.class);
						if (artifact.getId() == analysisId &&
						    artifact.getUserName().equalsIgnoreCase(userName) &&
						    artifact.getTenantId() == tenantId) {
							folder.delete();
							System.out.println(file.getName() + " is deleted!");

						}

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		for(MLProject project: projects){
			for(MLAnalysis analysis: project.getAnalyses()){
				if ((analysis.getId()==analysisId)){
					project.getAnalyses().remove(analysis);
				}
			}
		}

	}

	@Override public void updateModelError(long modelId, String error) throws DatabaseHandlerException {
		for(MLProject project : projects){
			for(MLAnalysis analysis : project.getAnalyses()){
				for(MLModelData model: analysis.getModels()){
					if(model.getId() == modelId){
						model.setError(error);
					}
				}
			}
		}
	}

	@Override public String getFeatureNamesInOrder(long datasetId, String columnSeparator) throws DatabaseHandlerException {
		String features = " ";
		StringJoiner joiner = new StringJoiner(columnSeparator);
		for(MLDataset dataset : datasets){
			if(dataset.getId() == datasetId){
				for(Feature feature : dataset.getDefaultFeatures()){
					joiner.add(feature.getName());
				}
			}
		}
		features = joiner.toString();
		return  features;
	}

	@Override public String getFeatureNamesInOrderUsingDatasetVersion(long datasetVersionId, String columnSeparator) throws DatabaseHandlerException {
		String features = " ";
		StringJoiner joiner = new StringJoiner(columnSeparator);
		for(MLDataset dataset : datasets){
			for(MLDatasetVersion version : dataset.getVersions()){
				if(version.getId()==datasetVersionId){
					for(Feature feature : dataset.getDefaultFeatures()){
						joiner.add(feature.getName());
					}
				}
			}
		}
		features = joiner.toString();
		return  features;
	}
	
	@Override public Map<String, String> getSummaryStats(long datasetVersionId) throws DatabaseHandlerException {
		Map<String,String>map = new HashMap<>();
		for(MLDataset dataset: datasets){
			for(MLDatasetVersion version :dataset.getVersions()){
				if(version.getId() == datasetVersionId){
					for(FeatureSummary feature: version.getFeatureSum()){
						map.put(feature.getFieldName(),feature.getSummaryStats());
					}
				}
			}
		}
		return map;
	}

	@Override public void shutdown() throws DatabaseHandlerException {


	}

	private JSONArray createJson(String type, SortedMap<?, Integer> graphFrequencies, int missing, int unique,
	                             DescriptiveStatistics descriptiveStats) throws JSONException {

		JSONObject json = new JSONObject();
		JSONArray freqs = new JSONArray();
		Object[] categoryNames = graphFrequencies.keySet().toArray();
		// Create an array with intervals/categories and their frequencies.
		for (int i = 0; i < graphFrequencies.size(); i++) {
			JSONArray temp = new JSONArray();
			temp.put(categoryNames[i].toString());
			temp.put(graphFrequencies.get(categoryNames[i]));
			freqs.put(temp);
		}
		// Put the statistics to a json object
		json.put("unique", unique);
		json.put("missing", missing);

		DecimalFormat decimalFormat = new DecimalFormat("#.###");
		if (descriptiveStats.getN() != 0) {
			json.put("mean", decimalFormat.format(descriptiveStats.getMean()));
			json.put("min", decimalFormat.format(descriptiveStats.getMin()));
			json.put("max", decimalFormat.format(descriptiveStats.getMax()));
			json.put("median", decimalFormat.format(descriptiveStats.getPercentile(50)));
			json.put("std", decimalFormat.format(descriptiveStats.getStandardDeviation()));
			if (type.equalsIgnoreCase(FeatureType.NUMERICAL)) {
				json.put("skewness", decimalFormat.format(descriptiveStats.getSkewness()));
			}
		}
		json.put("values", freqs);
		json.put("bar", true);
		json.put("key", "Frequency");
		JSONArray summaryStatArray = new JSONArray();
		summaryStatArray.put(json);
		return summaryStatArray;
	}
















}


