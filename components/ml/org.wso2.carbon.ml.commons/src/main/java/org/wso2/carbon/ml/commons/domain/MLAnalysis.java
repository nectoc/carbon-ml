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
package org.wso2.carbon.ml.commons.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;

/**
 * Represent an Analysis in ML.
 */
public class MLAnalysis {

    private long id;
    private long projectId;
    private String name;
    private int tenantId;
    private String userName;
    private String comments;
    //List of models per analysis
    @JsonDeserialize(as=ArrayList.class, contentAs=MLModelData.class)
    private List<MLModelData>models= new ArrayList<>();
    @JsonDeserialize(as=ArrayList.class, contentAs=MLHyperParameter.class)
    private List<MLHyperParameter> hyperParameters = new ArrayList<>();
    @JsonDeserialize(as=ArrayList.class, contentAs=MLModelConfiguration.class)
    private List<MLModelConfiguration> modelConfigurations = new ArrayList<>();
    @JsonDeserialize(as=ArrayList.class, contentAs=MLCustomizedFeature.class)
    private List<MLCustomizedFeature>features = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTenantId() {
        return tenantId;
    }

    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public long getProjectId() {
        return projectId;
    }

    public void setProjectId(long projectId) {
        this.projectId = projectId;
    }

    @Override
    public String toString() {
        return "MLAnalysis [projectId=" + projectId + ", name=" + name + ", tenantId=" + tenantId + ", userName="
                + userName + ", comments=" + comments + "]";
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<MLHyperParameter> getHyperParameters() {
        return hyperParameters;
    }

    public void setHyperParameters(List<MLHyperParameter> hyperParameters) {
        this.hyperParameters = hyperParameters;
    }

    public List<MLModelConfiguration> getModelConfigurations() {
        return modelConfigurations;
    }

    public void setModelConfigurations(List<MLModelConfiguration> modelConfigurations) {
        this.modelConfigurations = modelConfigurations;
    }

    public List<MLCustomizedFeature> getFeatures() {
        return features;
    }

    public void setFeatures(List<MLCustomizedFeature> features) {
        this.features = features;
    }

    public List<MLModelData> getModels() {
        return models;
    }

    public void setModels(List<MLModelData> models) {
        this.models = models;
    }
}
