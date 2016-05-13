package org.wso2.carbon.ml.core.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.wso2.carbon.ml.commons.domain.MLDataset;
import org.wso2.carbon.ml.commons.domain.MLDatasetVersion;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 5/6/16.
 */
public class MemoryModelHandler {

	public static List<MLDataset> datasets = new ArrayList<>();
	public static long datasetId = datasets.size();
	public static long versionId = 0;

	public List<MLDataset> addDatasets(MLDataset dataset){

		boolean exists = false;
		for(MLDataset set:datasets){
			if(set.getName().equalsIgnoreCase(dataset.getName())){
				exists = true;
			}
		}
		if(exists!=true){
			dataset.setId(datasets.size()+1);
			datasets.add(dataset);
		}

		return datasets;
	}

	public List<MLDataset> addVersions(MLDatasetVersion version){

//		version.setId(datasets.get((int) version.getDatasetId()).getVersions().size());
//		if(datasets.get((int) version.getDatasetId()).getVersions().size()==0){
//			datasets.get((int) version.getDatasetId()).setVersions(version);
//		}
//		datasets.get((int) version.getDatasetId()).getVersions().add(version);

		for(int i =0; i<datasets.size(); i++) {
			if(datasets.get(i).getId() == version.getDatasetId()){
				version.setId(datasets.get(i).getVersions().size()+1);
				datasets.get(i).getVersions().add(version);
			}
		}
		return datasets;
	}

	//Load versions to list on server startup
	public void getversions(File file) {

		ObjectMapper mapper = new ObjectMapper();
		try {
			// Convert JSON string from file to Object
			MLDatasetVersion version = mapper.readValue(file, MLDatasetVersion.class);
			datasets.get((int) version.getDatasetId()).setVersions(version);
			System.out.println("DatasetID in version : " + version.getDatasetId());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Load datasets to list on server startup
	public void getDatasets(File file){

		ObjectMapper mapper = new ObjectMapper();
		try {
			MLDataset dataset = mapper.readValue(file, MLDataset.class);
			datasets.add((int) dataset.getId(),dataset);
			System.out.println("DatasetID : " + dataset.getId());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadLists(File directoryName){

//		File directory = directoryName;
//		//get all the files from a directory
//		File[] fList = directory.listFiles();
//
//		if(fList.length!=0) {
//			for (File file : fList) {
//				if (file.isDirectory()) {
//					//Go inside version directory and load files to version objects
//					File[] files = file.listFiles();
//					for (File f : files) {
//						getversions(f);
//					}
//
//				} else {
//					getDatasets(file);
//				}
//
//			}
//		}
		loadDataset(directoryName);
		loadVersions(directoryName);

	}

	public void loadDataset(File directory){

		File[] list = directory.listFiles();
		if(list.length!=0){
			for(File file:list){
				if(file.isFile()){
					getDatasets(file);
				}
			}
		}
	}

	public void loadVersions(File directory){

		File[] list = directory.listFiles();
		if(list.length!=0){
			for(File file:list){
				if(file.isDirectory()){
					File[] files = file.listFiles();
					for (File f : files) {
						getversions(f);
					}
				}
			}
		}
	}

	public void listFilesForFolder(File location){
			//Files.walk(Paths.get(location)).filter(Files::isDirectory).forEach(System.out::println);

			for (final File fileEntry : location.listFiles()) {
				if (fileEntry.isDirectory()) {
					getversions(fileEntry);

					listFilesForFolder(fileEntry);
				} else {
					System.out.println(fileEntry.getName());
				}
			}
	}
	public List<MLDataset> insertDatasets(MLDataset dataset, MLDatasetVersion version) {

		if(datasets.size() == 0){
			dataset.setId(datasetId++);
			version.setId(1);
			dataset.getVersions().add(version);
			datasets.add(dataset);

		} else{
			for(int i=0; i<datasets.size();i++){
				if(datasets.get(i).getId()!=0){
					for(int j=0; i<datasets.get(i).getVersions().size();j++){
						version.setId(datasets.get(i).getVersions().size()+1);
						datasets.get(j).getVersions().add(version);
					}
				}else{
					datasetId++;
					version.setId(1);
					dataset.getVersions().add(version);
					datasets.add(dataset);

				}
			}
		}

		return datasets;
	}



}
