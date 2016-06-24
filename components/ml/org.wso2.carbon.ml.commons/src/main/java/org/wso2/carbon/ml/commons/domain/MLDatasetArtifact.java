package org.wso2.carbon.ml.commons.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 6/24/16.
 */
public class MLDatasetArtifact {

	private long id;
	private String name;
	private int tenantId;
	private String userName;
	@JsonDeserialize(as=ArrayList.class, contentAs=Feature.class)
	private List<Feature> defaultFeatures;
	/*
	 * Originated path of the data-set.
	 */
	private String sourcePath;
	/*
	 * Data type i.e. CSV, TSV
	 */
	private String dataType;
	private String comments;

	private boolean containsHeader;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

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

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public List<Feature> getDefaultFeatures() {
		return defaultFeatures;
	}

	public void setDefaultFeatures(List<Feature> defaultFeatures) {
		this.defaultFeatures = defaultFeatures;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public boolean isContainsHeader() {
		return containsHeader;
	}

	public void setContainsHeader(boolean containsHeader) {
		this.containsHeader = containsHeader;
	}
}
