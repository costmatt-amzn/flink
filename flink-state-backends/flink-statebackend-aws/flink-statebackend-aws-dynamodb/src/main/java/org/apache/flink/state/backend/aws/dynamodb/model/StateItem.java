/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.dynamodb.model;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * An immutable class representing a state item in DynamoDB.
 *
 * <p>This class is used with the DynamoDB Enhanced Client to map between
 * Java objects and DynamoDB items. It represents a single state entry in the
 * DynamoDB table used by the Flink state backend.
 */
@DynamoDbBean
@SuppressWarnings("unused")
public class StateItem {

    private String jobAwareStateKeyAndDescriptor; // Partition key
    private String namespace;                    // Sort key
    private String jobNameAndTaskName;           // GSI partition key
    private SdkBytes value;
    private Long ttl;
    private String stateDescriptor;              // GSI sort key
    private String jobId;
    private String jobName;
    private String taskName;
    private int taskIndex;
    private String operatorId;
    private String stateKey;

    /**
     * Default constructor required by the DynamoDB Enhanced Client.
     */
    public StateItem() {
    }

    /**
     * Private constructor used by the Builder.
     */
    private StateItem(final Builder builder) {
        this.jobAwareStateKeyAndDescriptor = builder.jobAwareStateKeyAndDescriptor;
        this.namespace = builder.namespace;
        this.jobNameAndTaskName = builder.jobNameAndTaskName;
        this.value = builder.value;
        this.ttl = builder.ttl;
        this.stateDescriptor = builder.stateDescriptor;
        this.jobId = builder.jobId;
        this.jobName = builder.jobName;
        this.taskName = builder.taskName;
        this.taskIndex = builder.taskIndex;
        this.operatorId = builder.operatorId;
        this.stateKey = builder.stateKey;
    }

    /**
     * Creates a new builder for StateItem.
     *
     * @return A new StateItem builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @DynamoDbPartitionKey
    public String getJobAwareStateKeyAndDescriptor() {
        return jobAwareStateKeyAndDescriptor;
    }

    public void setJobAwareStateKeyAndDescriptor(final String jobAwareStateKeyAndDescriptor) {
        this.jobAwareStateKeyAndDescriptor = jobAwareStateKeyAndDescriptor;
    }

    @DynamoDbSortKey
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    public String getJobNameAndTaskName() {
        return jobNameAndTaskName;
    }

    public void setJobNameAndTaskName(final String jobNameAndTaskName) {
        this.jobNameAndTaskName = jobNameAndTaskName;
    }

    // Removed backward compatibility methods

    public SdkBytes getValue() {
        return value;
    }

    public void setValue(final SdkBytes value) {
        this.value = value;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(final Long ttl) {
        this.ttl = ttl;
    }

    public String getStateDescriptor() {
        return stateDescriptor;
    }

    public void setStateDescriptor(final String stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(final String taskName) {
        this.taskName = taskName;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(final int taskIndex) {
        this.taskIndex = taskIndex;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(final String operatorId) {
        this.operatorId = operatorId;
    }

    // Removed duplicate namespace getter/setter that was causing compilation errors

    public String getStateKey() {
        return stateKey;
    }

    public void setStateKey(final String stateKey) {
        this.stateKey = stateKey;
    }

    /**
     * Builder for creating StateItem instances.
     */
    public static class Builder {
        private String jobAwareStateKeyAndDescriptor;
        private String namespace;
        private String jobNameAndTaskName;
        private SdkBytes value;
        private Long ttl;
        private String stateDescriptor;
        private String jobId;
        private String jobName;
        private String taskName;
        private int taskIndex;
        private String operatorId;
        private String stateKey;

        public Builder jobAwareStateKeyAndDescriptor(final String jobAwareStateKeyAndDescriptor) {
            this.jobAwareStateKeyAndDescriptor = jobAwareStateKeyAndDescriptor;
            return this;
        }

        public Builder namespace(final String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder jobNameAndTaskName(final String jobNameAndTaskName) {
            this.jobNameAndTaskName = jobNameAndTaskName;
            return this;
        }

        // Removed backward compatibility builder methods

        public Builder value(final SdkBytes value) {
            this.value = value;
            return this;
        }

        public Builder ttl(final Long ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder stateDescriptor(final String stateDescriptor) {
            this.stateDescriptor = stateDescriptor;
            return this;
        }

        public Builder jobId(final String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder jobName(final String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder taskName(final String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder taskIndex(final int taskIndex) {
            this.taskIndex = taskIndex;
            return this;
        }

        public Builder operatorId(final String operatorId) {
            this.operatorId = operatorId;
            return this;
        }

        public Builder stateKey(final String stateKey) {
            this.stateKey = stateKey;
            return this;
        }

        public StateItem build() {
            return new StateItem(this);
        }
    }

}
