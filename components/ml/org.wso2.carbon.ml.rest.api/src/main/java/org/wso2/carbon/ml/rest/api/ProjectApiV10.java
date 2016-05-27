/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.ml.rest.api;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.ml.commons.domain.MLAnalysis;
import org.wso2.carbon.ml.commons.domain.MLModelData;
import org.wso2.carbon.ml.commons.domain.MLProject;
import org.wso2.carbon.ml.core.exceptions.MLProjectHandlerException;
import org.wso2.carbon.ml.core.impl.MLProjectHandler;
import org.wso2.carbon.ml.core.utils.MLUtils;
import org.wso2.carbon.ml.rest.api.model.MLAnalysisBean;
import org.wso2.carbon.ml.rest.api.model.MLErrorBean;
import org.wso2.carbon.ml.rest.api.model.MLProjectBean;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is to handle REST verbs GET , POST and DELETE.
 */
@Path("/projects")
public class ProjectApiV10 extends MLRestAPI {

    private static final Log logger = LogFactory.getLog(ProjectApiV10.class);
    private MLProjectHandler mlProjectHandler;

    public ProjectApiV10() {
        mlProjectHandler = new MLProjectHandler();
    }

    @OPTIONS
    public Response options() {
        return Response.ok().header(HttpHeaders.ALLOW, "GET POST DELETE").build();
    }

    /**
     * Create a new Project. No validation happens here. Please call {@link #getProject(String)} before this.
     * @param project {@link org.wso2.carbon.ml.commons.domain.MLProject} object
     */
    @POST
    @Produces("application/json")
    @Consumes("application/json")
    public Response createProject(MLProject project) {
        if (project.getName() == null || project.getName().isEmpty() || project.getDatasetName() == null
                || project.getDatasetName().isEmpty()) {
            logger.error("Required parameters missing");
            return Response.status(Response.Status.BAD_REQUEST).entity("Required parameters missing").build();
        }
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            project.setTenantId(tenantId);
            project.setUserName(userName);
            mlProjectHandler.createProject(project);
            return Response.ok().build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while creating a [project] %s of tenant [id] %s and [user] %s .", project,
                    tenantId, userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get project by project name
     * @param projectName Name of the project
     * @return JSON of {@link org.wso2.carbon.ml.commons.domain.MLProject} object
     */
    @GET
    @Path("/{projectName}")
    @Produces("application/json")
    public Response getProject(@PathParam("projectName") String projectName) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            MLProject project = mlProjectHandler.getProject(tenantId, userName, projectName);
            if (project == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(project).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while retrieving a project [name] %s of tenant [id] %s and [user] %s .",
                    projectName, tenantId, userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get all projects
     * @return JSON array of {@link org.wso2.carbon.ml.commons.domain.MLProject} objects
     */
    @GET
    @Produces("application/json")
    public Response getAllProjects() {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            List<MLProject> projects = mlProjectHandler.getAllProjects(tenantId, userName);
            return Response.ok(projects).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while retrieving all projects of tenant [id] %s and [user] %s .", tenantId,
                    userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get all models of a project
     * @param projectId Unique id of the project
     * @return JSON array of {@link org.wso2.carbon.ml.commons.domain.MLModelData} objects
     */
    @GET
    @Path("/{projectId}/models")
    @Produces("application/json")
    public Response getProjectModels(@PathParam("projectId") long projectId) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            List<MLModelData> models = mlProjectHandler.getProjectModels(tenantId, userName, projectId);
            return Response.ok(models).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while retrieving all models of project [id]  %s of tenant [id] %s and [user] %s .",
                    projectId, tenantId, userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get all projects created with the given dataset
     * @param datasetName Name of the dataset
     * @return JSON array of {@link org.wso2.carbon.ml.rest.api.model.MLProjectBean} objects
     */
    @GET
    @Path("/analyses")
    @Produces("application/json")
    public Response getAllProjectsWithAnalyses(@QueryParam("datasetName") String datasetName) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            List<MLProject> projects = mlProjectHandler.getAllProjects(tenantId, userName);
            List<MLProjectBean> projectBeans = new ArrayList<MLProjectBean>();
            for (MLProject mlProject : projects) {
                if (!StringUtils.isEmpty(datasetName) && !datasetName.equals(mlProject.getDatasetName())) {
                    continue;
                }
                MLProjectBean projectBean = new MLProjectBean();
                long projectId = mlProject.getId();
                projectBean.setId(projectId);
                projectBean.setCreatedTime(mlProject.getCreatedTime());
                projectBean.setDatasetId(mlProject.getDatasetId());
                projectBean.setDatasetName(mlProject.getDatasetName());
                projectBean.setDatasetStatus(mlProject.getDatasetStatus());
                projectBean.setDescription(mlProject.getDescription());
                projectBean.setName(mlProject.getName());

                List<MLAnalysisBean> analysisBeans = new ArrayList<MLAnalysisBean>();
                List<MLAnalysis> analyses = mlProjectHandler.getAllAnalysesOfProject(tenantId, userName, projectId);
                for (MLAnalysis mlAnalysis : analyses) {
                    MLAnalysisBean analysisBean = new MLAnalysisBean();
                    analysisBean.setId(mlAnalysis.getId());
                    analysisBean.setName(mlAnalysis.getName());
                    analysisBean.setProjectId(mlAnalysis.getProjectId());
                    analysisBean.setComments(mlAnalysis.getComments());
                    analysisBeans.add(analysisBean);
                }
                projectBean.setAnalyses(analysisBeans);
                projectBeans.add(projectBean);
            }
            return Response.ok(projectBeans).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while retrieving all analyses of tenant [id] %s and [user] %s .", tenantId,
                    userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get all analyses of a project
     * @param projectId Unique id of the project
     * @return A JSON array of {@link org.wso2.carbon.ml.commons.domain.MLAnalysis} objects
     */
    @GET
    @Path("/{projectId}/analyses")
    @Produces("application/json")
    public Response getAllAnalysesOfProject(@PathParam("projectId") long projectId) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            List<MLAnalysis> analyses = mlProjectHandler.getAllAnalysesOfProject(tenantId, userName, projectId);
            return Response.ok(analyses).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils
                    .getErrorMsg(
                            String.format(
                                    "Error occurred while retrieving all analyses of project [id]  %s of tenant [id] %s and [user] %s .",
                                    projectId, tenantId, userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Get analysis of a project given the analysis name
     * @param projectId Unique id of the project
     * @param analysisName Name of the analysis
     * @return JSON of {@link org.wso2.carbon.ml.commons.domain.MLAnalysis} object
     */
    @GET
    @Path("/{projectId}/analyses/{analysisName}")
    @Produces("application/json")
    public Response getAnalysisOfProject(@PathParam("projectId") long projectId,
            @PathParam("analysisName") String analysisName) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            MLAnalysis analysis = mlProjectHandler.getAnalysisOfProject(tenantId, userName, projectId, analysisName);
            if (analysis == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(analysis).build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils
                    .getErrorMsg(
                            String.format(
                                    "Error occurred while retrieving analysis with [name] %s of project [id]  %s of tenant [id] %s and [user] %s .",
                                    analysisName, projectId, tenantId, userName), e);
            logger.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

    /**
     * Delete a project
     * @param projectId Unique id of the project
     */
    @DELETE
    @Path("/{projectId}")
    @Produces("application/json")
    public Response deleteProject(@PathParam("projectId") long projectId) {
        PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
        int tenantId = carbonContext.getTenantId();
        String userName = carbonContext.getUsername();
        try {
            mlProjectHandler.deleteProject(tenantId, userName, projectId);
            auditLog.info(String.format("User [name] %s of tenant [id] %s deleted a project [id] %s ", userName,
                    tenantId, projectId));
            return Response.ok().build();
        } catch (MLProjectHandlerException e) {
            String msg = MLUtils.getErrorMsg(String.format(
                    "Error occurred while deleting a project [id]  %s of tenant [id] %s and [user] %s .", projectId,
                    tenantId, userName), e);
            logger.error(msg, e);
            auditLog.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new MLErrorBean(e.getMessage()))
                    .build();
        }
    }

}
