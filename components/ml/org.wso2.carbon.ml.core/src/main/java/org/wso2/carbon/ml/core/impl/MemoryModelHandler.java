package org.wso2.carbon.ml.core.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.wso2.carbon.ml.commons.domain.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 5/6/16.
 */
public class MemoryModelHandler {

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


































	}