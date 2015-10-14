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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.wso2.carbon.ml.commons.domain.Feature;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.internal.MLModelConfigurationContext;
import org.wso2.carbon.ml.core.spark.algorithms.SparkModelUtils;

/**
 * This class normalize the each values row by row
 * Using this you can normalize numerical features
 */
public class Normalization implements Function<double[], double[]> {

    private static final long serialVersionUID = 8329428281317101710L;
    private final List<Double> max;
    private final List<Double> min;

    public Normalization(Builder builder) {
        this.max = builder.max;
        this.min = builder.min;
    }

    @Override
    public double[] call(double[] values) throws MLModelBuilderException {

        try {
            double[] normalizedValues = new double[values.length];

            for (int i = 0; i < values.length; i++) {

                if (values[i] > max.get(i)) {
                    normalizedValues[i] = 1;
                } else if (values[i] < min.get(i)) {
                    normalizedValues[i] = 0;
                } else if (min.get(i) == max.get(i)) {
                    normalizedValues[i] = 0.5;
                } else {
                    normalizedValues[i] = (values[i] - min.get(i)) / (max.get(i) - min.get(i));
                }
            }

            return normalizedValues;

        } catch (Exception e) {
            throw new MLModelBuilderException("An error occurred while normalizing values: " + e.getMessage(), e);
        }
    }

    public static class Builder {
        private List<Double> max = new ArrayList<Double>();
        private List<Double> min = new ArrayList<Double>();

        public Builder init(MLModelConfigurationContext ctx) {

            List<Feature> features = ctx.getFacts().getIncludedFeatures();
            Map<String, String> stats = ctx.getSummaryStatsOfFeatures();

            for (Feature feature : features) {

                String featureStat = stats.get(feature.getName());
                double maxValue = SparkModelUtils.getMax(featureStat);
                this.max.add(maxValue);
                double minValue = SparkModelUtils.getMin(featureStat);
                this.min.add(minValue);
            }
            return this;
        }

        public Builder minMax(List<Feature> features, Map<String, String> stats) {

            for (Feature feature : features) {

                String featureStat = stats.get(feature.getName());
                double maxValue = SparkModelUtils.getMax(featureStat);
                this.max.add(maxValue);
                double minValue = SparkModelUtils.getMin(featureStat);
                this.min.add(minValue);
            }
            return this;
        }

        public Normalization build() {
            return new Normalization(this);
        }
    }
}
