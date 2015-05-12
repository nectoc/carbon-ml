/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.ml.core.spark.transformations;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;

import java.util.List;
import java.util.Map;

/**
 * This class transforms double array of tokens to labeled point
 */
public class DoubleArrayToLabeledPoint implements Function<double[], LabeledPoint> {

    private static final long serialVersionUID = -3847503088002249546L;
    private final Map<Integer, String> includedFeatures;
    private final int responseIndex;

    /**
     * @param index Index of the response variable
     */
    public DoubleArrayToLabeledPoint(Map<Integer, String> includedFeatures, int responseIndex) {
        this.includedFeatures = includedFeatures;
        this.responseIndex = responseIndex;
    }

    /**
     * Function to transform double array into labeled point
     *
     * @param tokens    Double array of tokens
     * @return          Labeled point
     * @throws          ModelServiceException
     */
    @Override
    public LabeledPoint call(double[] tokens) throws MLModelBuilderException {
        try {
            double response = tokens[responseIndex];
            double[] features = new double[includedFeatures.size()];
            int featureIndex = 0;
            for (int i = 0; i < tokens.length-1; i++) {
                // if not response
                if (includedFeatures.containsKey(i)) {
                    features[featureIndex] = tokens[i];
                    featureIndex++;
                }
            }
            return new LabeledPoint(response, Vectors.dense(features));
        } catch (Exception e) {
            throw new MLModelBuilderException("An error occurred while transforming double array to labeled point: "
                    + e.getMessage(), e);
        }
    }
}