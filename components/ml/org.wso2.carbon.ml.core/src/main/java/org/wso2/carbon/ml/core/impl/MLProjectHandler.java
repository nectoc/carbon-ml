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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ml.commons.domain.MLAnalysis;
import org.wso2.carbon.ml.commons.domain.MLModelData;
import org.wso2.carbon.ml.commons.domain.MLProject;
import org.wso2.carbon.ml.core.exceptions.MLProjectHandlerException;
import org.wso2.carbon.ml.core.utils.MLCoreServiceValueHolder;
import org.wso2.carbon.ml.database.DatabaseService;
import org.wso2.carbon.ml.database.exceptions.DatabaseHandlerException;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * {@link MLProjectHandler} is responsible for handling/delegating all the project management requests.
 */
public class MLProjectHandler {
    private static final Log log = LogFactory.getLog(MLProjectHandler.class);
    private DatabaseService databaseService;
    MemoryModelHandler memoryModelHandler = new MemoryModelHandler();

    public MLProjectHandler() {
        databaseService = MLCoreServiceValueHolder.getInstance().getDatabaseService();
    }
    
    public void createProject(MLProject project) throws MLProjectHandlerException {
        try {
            long datasetId =  memoryModelHandler.getDatasetId(project.getDatasetName(), project.getTenantId(),
                    project.getUserName());
            if (datasetId == -1) {
                throw new MLProjectHandlerException("Invalid dataset [name] " + project.getDatasetName());
            }
            project.setDatasetId(datasetId);
            createProjectArtifact(project);
            databaseService.insertProject(project);
            log.info(String.format("[Created] %s", project));
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }
    
    public void deleteProject(int tenantId, String userName, long projectId) throws MLProjectHandlerException {
        try {
            memoryModelHandler.deleteProject(tenantId, userName, projectId);
            log.info(String.format("[Deleted] [project] %s of [user] %s of [tenant] %s", projectId, userName, tenantId));
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }
    
    public MLProject getProject(int tenantId, String userName, String projectName) throws MLProjectHandlerException {
        try {

            return  memoryModelHandler.getProject(tenantId, userName, projectName);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }

    public MLProject getProject(int tenantId, String userName, long projectId) throws MLProjectHandlerException {
        try {
            return databaseService.getProject(tenantId, userName, projectId);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }
    
    public List<MLProject> getAllProjects(int tenantId, String userName) throws MLProjectHandlerException {
        try {
            return  memoryModelHandler.getAllProjects(tenantId, userName);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }

    public List<MLModelData> getProjectModels(int tenantId, String userName, long projectId) throws MLProjectHandlerException {
        try {
            return  memoryModelHandler.getProjectModels(tenantId, userName, projectId);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }
    
    public List<MLAnalysis> getAllAnalysesOfProject(int tenantId, String userName, long projectId) throws MLProjectHandlerException {
        try {
            return  memoryModelHandler.getAllAnalysesOfProject(tenantId, userName, projectId);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }

    public MLAnalysis getAnalysisOfProject(int tenantId, String userName, long projectId, String analysisName) throws MLProjectHandlerException {
        try {
            return  memoryModelHandler.getAnalysisOfProject(tenantId, userName, projectId, analysisName);
        } catch (DatabaseHandlerException e) {
            throw new MLProjectHandlerException(e.getMessage(), e);
        }
    }

    public void createProjectArtifact(MLProject project){

        MemoryModelHandler model = new MemoryModelHandler();
        List<MLProject> projects = model.addProjects(project);
        ObjectMapper mapper = new ObjectMapper();
        MLProject set = projects.get(projects.size()-1);

        File dir = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "projects" + File.separator + project.getName());
        if (!dir.exists()) {
            if (dir.mkdir()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        try {
            File file = new File(System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "deployment" + File.separator + "server" + File.separator + "projects" + File.separator + project.getName()+File.separator + project.getName() + ".json");
            if(!file.exists()) {
                mapper.writeValue(new File(
                        System.getProperty("carbon.home") + File.separator + "repository" +
                        File.separator + "deployment" + File.separator + "server" + File.separator +
                        "projects" + File.separator + project.getName() + File.separator + project.getName() +".json"), set);
                String jsonInString = mapper.writeValueAsString(set);
                System.out.println(jsonInString);

                // Convert object to JSON string and print
                jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(set);
                System.out.println(jsonInString);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
