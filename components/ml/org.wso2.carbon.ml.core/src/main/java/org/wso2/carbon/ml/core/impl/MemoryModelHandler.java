package org.wso2.carbon.ml.core.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.wso2.carbon.ml.commons.domain.MLAnalysis;
import org.wso2.carbon.ml.commons.domain.MLDataset;
import org.wso2.carbon.ml.commons.domain.MLDatasetVersion;
import org.wso2.carbon.ml.commons.domain.MLProject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 5/6/16.
 */
public class MemoryModelHandler {

	public static List<MLDataset> datasets = new ArrayList<>();
	public static List<MLProject> projects = new ArrayList<>();

	public List<MLDataset> addDatasets(MLDataset dataset){

		boolean exists = false;
		for(MLDataset set:datasets){
			if(set.getName().equalsIgnoreCase(dataset.getName())){
				exists = true;
			}
		}
		if(exists!=true){
			dataset.setId(datasets.size()+1);
			//Adding id 1 for 0th index ;
			datasets.add(dataset);
		}

		return datasets;
	}

	public List<MLDataset> addVersions(MLDatasetVersion version){

		for(int i =0; i<datasets.size(); i++) {
			if(datasets.get(i).getId() == version.getDatasetId()){
				if(!datasets.get(i).getVersions().contains(version.getName())){
					version.setId(datasets.get(i).getVersions().size() + 1);
					datasets.get(i).getVersions().add(version);

				}
				List<MLDatasetVersion> ver = datasets.get(i).getVersions();
				for(int j = 0 ; j<datasets.get(i).getVersions().size(); j++){
					System.out.println("Id : " + ver.get(j).getId()+ "Name : " + ver.get(j).getName()+ "index when adding : " + j);
				}
			}
		}
		return datasets;
	}

	public List<MLProject>addProjects(MLProject project){

		boolean exists = false;
		for(MLProject set:projects){
			if(set.getName().equalsIgnoreCase(project.getName())){
				exists = true;
			}
		}
		if(exists!=true){
			project.setId(projects.size()+1);
			//Adding id 1 for 0th index ;
			projects.add(project);
		}
		return projects;
	}

	public List<MLProject> addAnalyses(MLAnalysis analysis){

		for(int i =0; i<projects.size(); i++) {
			if(projects.get(i).getId() == analysis.getProjectId()){
				if(!projects.get(i).getAnalyses().contains(analysis.getName())){
					analysis.setId(projects.get(i).getAnalyses().size() + 1);
					projects.get(i).getAnalyses().add(analysis);

				}
				List<MLAnalysis> analysisList = projects.get(i).getAnalyses();
				for(int j = 0 ; j<projects.get(i).getAnalyses().size(); j++){
					System.out.println("Id : " + analysisList.get(j).getId()+ "Name : " + analysisList.get(j).getName()+ "index when adding : " + j);
				}
			}
		}

		return projects;
	}

	public void getAllDatasets(){
		ObjectMapper mapper = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets" );
		File []files = location.listFiles();
		List<MLDataset> datasetList = new ArrayList<>();

		if(files.length!=0 && datasets.size()==0){

			for(File f: files){
				if(f.isFile()){
					try {
						MLDataset dataset = mapper.readValue(f, MLDataset.class);
						datasetList.add(dataset);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			MLDataset[] datasetArr = new MLDataset[datasetList.size()];
			datasetList.toArray(datasetArr);
			MLDataset[]arr = sortDatasets(datasetArr);

			for(MLDataset set : arr){
				datasets.add(set);
			}
			getAllVersions();
		}

	}

	public MLDataset[] sortDatasets(MLDataset[] input){
		MLDataset temp;
		for (int i = 1; i < input.length; i++) {
			for(int j = i ; j > 0 ; j--){
				if(input[j].getId() < input[j-1].getId()){
					temp = input[j];
					input[j] = input[j-1];
					input[j-1] = temp;
				}
			}
		}
		return input;

	}

	public MLDatasetVersion[] sortVersions(MLDatasetVersion[] input){
		MLDatasetVersion temp;
		for (int i = 1; i < input.length; i++) {
			for(int j = i ; j > 0 ; j--){
				if(input[j].getId() < input[j-1].getId()){
					temp = input[j];
					input[j] = input[j-1];
					input[j-1] = temp;
				}
			}
		}
		return input;

	}

	public void getAllVersions(){

		ObjectMapper mapper  = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "datasets" );
		File[] list = location.listFiles();
		List<MLDatasetVersion> versionList;

		for(File file:list){
			if(file.isDirectory()){
				//Always assigning version list to new array since every iteration of list file array contains a different set of datasets
				versionList = new ArrayList<>();
				File[] files = file.listFiles();

				for (File f : files) {
					try {
						MLDatasetVersion version = mapper.readValue(f, MLDatasetVersion.class);
						versionList.add(version);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				MLDatasetVersion[]versions = new MLDatasetVersion[versionList.size()];
				versionList.toArray(versions);
				MLDatasetVersion[]arr = sortVersions(versions);

				for(MLDataset set : datasets){
					//Version list always contains a list of version for a single dataset as it goes through one folder at a time
					if(versionList.get(0).getDatasetId()== set.getId()){
						for(MLDatasetVersion versionNew : arr){
							set.getVersions().add(versionNew);
						}
					}
				}
			}
		}

	}

	public void getProjects(){

		ObjectMapper mapper = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "projects" );
		File []files = location.listFiles();
		List<MLProject> projectList = new ArrayList<>();

		if(files.length!=0 && projects.size()==0){
			for(File f: files){
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
			MLProject[]arr = sortProjects(projectArr);

			for(MLProject set : arr){
				projects.add(set);
			}
			getAnalyses();
		}
		
	}

	public MLProject[] sortProjects(MLProject[] input){
		MLProject temp;
		for (int i = 1; i < input.length; i++) {
			for(int j = i ; j > 0 ; j--){
				if(input[j].getId() < input[j-1].getId()){
					temp = input[j];
					input[j] = input[j-1];
					input[j-1] = temp;
				}
			}
		}
		return input;

	}

	//Edit this
	public void getAnalyses(){

		ObjectMapper mapper  = new ObjectMapper();
		File location = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "analyses" );
		File[] list = location.listFiles();
		List<MLAnalysis> analysisList;

		for(File file:list){
			if(file.isDirectory()){
				analysisList = new ArrayList<>();
				File[] files = file.listFiles();

				for (File f : files) {
					try {
						MLAnalysis analysis = mapper.readValue(f, MLAnalysis.class);
						analysisList.add(analysis);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				MLAnalysis[] analyses = new MLAnalysis[analysisList.size()];
				analysisList.toArray(analyses);
				MLAnalysis[]arr = sortAnalyses(analyses);

				for(MLProject set : projects){
					if(analysisList.get(0).getProjectId()== set.getId()){
						for(MLAnalysis analysisNew : arr){
							set.getAnalyses().add(analysisNew);
						}
					}
				}
			}
		}
	}

	public MLAnalysis[] sortAnalyses(MLAnalysis[] input){
		MLAnalysis temp;
		for (int i = 1; i < input.length; i++) {
			for(int j = i ; j > 0 ; j--){
				if(input[j].getId() < input[j-1].getId()){
					temp = input[j];
					input[j] = input[j-1];
					input[j-1] = temp;
				}
			}
		}
		return input;

	}


}
