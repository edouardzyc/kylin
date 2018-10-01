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

package org.apache.kylin.rest.metrics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class QueryMetricsContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    protected static final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    public static final String DB_NAME = "KAP_METRIC";
    public static final String QUERY_MEASUREMENT = "query_metric";
    public static final String REALIZATION_MEASUREMENT = "realization_metric";

    private static final String QUERY_ID_METRIC = "query_id";
    private static final String SQL_TEXT_METRIC = "sql_text";
    private static final String QUERY_DURATION_METRIC = "query_duration";
    private static final String TOTAL_SCAN_BYTES_METRIC = "total_scan_bytes";
    private static final String SUBMITTER_METRIC = "submitter";
    private static final String PROJECT_METRIC = "project";
    private static final String MODEL_METRIC = "model";
    private static final String REALIZATION_NAME_METRIC = "realization_name";
    private static final String REALIZATION_TYPE_METRIC = "realization_type";
    private static final String HOSTNAME_METRIC = "hostname";
    private static final String SUITE_METRIC = "suite";
    private static final String ERROR_TYPE_METRIC = "error_type";
    private static final String ENGINE_TYPE_METRIC = "engine_type";

    private static final String REALIZATIONS_METRIC = "realizations";

    private static final String LOG_METRIC = "log";

    private static final InheritableThreadLocal<QueryMetricsContext> contexts = new InheritableThreadLocal<>();

    private final String queryId;

    private String sql;

    private String submitter;
    private String project;
    private String hostname;
    private String suite;

    private long queryDuration;
    private long totalScanBytes;

    private String errorType;

    private String engineType;

    private final Set<RealizationMetrics> realizationMetrics = new HashSet<>();

    private String log;

    private QueryMetricsContext(String queryId) {
        this.queryId = queryId;
    }

    public static void start(final String queryId) {
        if (!isCollectEnabled()) {
            logger.warn(
                    "Can't to start QueryMetricsContext, please set kap.metric.diagnosis.graph-writer-type to 'INFLUX'");
            return;
        }

        if (isStarted()) {
            logger.warn("Query metric context already started in thread named {}", Thread.currentThread().getName());
            return;
        }
        contexts.set(new QueryMetricsContext(queryId));
    }

    private static boolean isCollectEnabled() {
        return "INFLUX".equals(kylinConfig.diagnosisMetricWriterType());
    }

    public static boolean isStarted() {
        return contexts.get() != null;
    }

    public static QueryMetricsContext collect(final SQLRequest request, final SQLResponse response,
            final QueryContext context) {
        final QueryMetricsContext current = obtainCurrentQueryMetrics();

        current.doCollect(request, response, context);

        return current;
    }

    public static void log(final String log) {
        obtainCurrentQueryMetrics().log = log;
    }

    public static void reset() {
        contexts.remove();
    }

    private static QueryMetricsContext obtainCurrentQueryMetrics() {
        final QueryMetricsContext current = contexts.get();
        Preconditions.checkState(current != null, "Query metric context is not started.");
        return current;
    }

    private void doCollect(final SQLRequest request, final SQLResponse response, final QueryContext context) {
        this.sql = request.getSql();

        this.submitter = request.getUsername();
        this.project = request.getProject();
        this.hostname = response.getServer();
        this.suite = response.getSuiteId();

        this.queryDuration = response.getDuration();
        this.totalScanBytes = response.getTotalScanBytes();

        collectErrorType(context);
        collectRealizationMetrics(response, context);
        collectEngineType(response);

        logger.debug("Query[{}] collect metrics {}, {}, {}, {}, {}, {}, {}, {}, {}", queryId, sql, submitter, project,
                hostname, suite, queryDuration, totalScanBytes, errorType, engineType);
    }

    private void collectErrorType(final QueryContext context) {
        Throwable cause = context.getErrorCause();
        while (cause != null) {
            if (cause instanceof SqlValidatorException || cause instanceof SqlParseException
                    || cause.getClass().getName().contains("ParseException")) {
                this.errorType = "Syntax error";
                return;

            } else if (cause instanceof NoRealizationFoundException) {
                this.errorType = "No realization found";
                return;
            }

            cause = cause.getCause();
        }

        if (context.getErrorCause() != null) {
            this.errorType = "Other error";
        }
    }

    public Set<RealizationMetrics> getRealizationMetrics() {
        return ImmutableSet.copyOf(realizationMetrics);
    }

    private void collectRealizationMetrics(final SQLResponse response, final QueryContext context) {
        if (response.isHitExceptionCache() || null == OLAPContext.getThreadLocalContexts()) {
            logger.debug("Query[{}] hit cache or can't find OLAPContext.", context.getQueryId());
            return;
        }

        for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
            if (ctx.realization != null) {
                addRealizationMetrics(ctx.realization.getName(), ctx.realization.getType().toString(),
                        ctx.realization.getModel() == null ? "UNKNOWN" : ctx.realization.getModel().getName());
            }
        }
    }

    private void addRealizationMetrics(String realizationName, String realizationType, String modelName) {
        realizationMetrics
                .add(new RealizationMetrics(queryId, suite, project, realizationName, realizationType, modelName));
        logger.debug("Query[{}] hit project [{}], model [{}], realization name [{}], realization type [{}]", queryId,
                project, modelName, realizationName, realizationType);
    }

    private void collectEngineType(final SQLResponse response) {
        if (response.isPushDown()) {
            this.engineType = "Pushdown";
        } else if (!realizationMetrics.isEmpty()) {
            this.engineType = realizationMetrics.iterator().next().realizationType;

            // correct engine name
            if (RealizationType.INVERTED_INDEX.name().equals(engineType)) {
                this.engineType = "Table Index";
            }
        } else {
            this.engineType = "Unknown";
        }
    }

    public Map<String, String> getInfluxdbTags() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String> builder() //
                .put(SUBMITTER_METRIC, submitter) //
                .put(PROJECT_METRIC, project) //
                .put(SUITE_METRIC, suite == null ? "Unknown" : suite) //
                .put(ENGINE_TYPE_METRIC, engineType);

        if (StringUtils.isBlank(hostname)) {
            try {
                hostname = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                hostname = "Unknown";
            }
        }
        builder.put(HOSTNAME_METRIC, hostname);

        if (StringUtils.isNotBlank(this.errorType)) {
            builder.put(ERROR_TYPE_METRIC, errorType);
        }

        return builder.build();
    }

    public Map<String, Object> getInfluxdbFields() {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object> builder() //
                .put(QUERY_ID_METRIC, queryId) //
                .put(SQL_TEXT_METRIC, sql) //
                .put(QUERY_DURATION_METRIC, queryDuration) //
                .put(TOTAL_SCAN_BYTES_METRIC, totalScanBytes);

        if (!realizationMetrics.isEmpty()) {
            final Collection<String> realizations = Collections2.transform(realizationMetrics,
                    new Function<RealizationMetrics, String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable RealizationMetrics input) {
                            return input.realizationName;
                        }
                    });

            builder.put(REALIZATIONS_METRIC, Joiner.on(",").join(realizations));
        }

        if (StringUtils.isNotBlank(this.log)) {
            builder.put(LOG_METRIC, log);
        }

        return builder.build();
    }

    public static class RealizationMetrics {

        private String queryId;

        private String suite;

        private String project;

        private String realizationName;

        private String realizationType;

        private String modelName;

        public RealizationMetrics(String queryId, String suite, String project, String realizationName,
                String realizationType, String modelName) {
            this.queryId = queryId;
            this.suite = suite == null ? "Unknown" : suite;
            this.project = project;
            this.realizationName = realizationName;
            this.realizationType = realizationType;
            this.modelName = modelName;
        }

        public Map<String, String> getInfluxdbTags() {
            return ImmutableMap.<String, String> builder() //
                    .put(SUITE_METRIC, suite) //
                    .put(PROJECT_METRIC, project) //
                    .put(MODEL_METRIC, modelName) //
                    .put(REALIZATION_NAME_METRIC, realizationName) //
                    .put(REALIZATION_TYPE_METRIC, realizationType) //
                    .build();
        }

        public Map<String, Object> getInfluxdbFields() {
            return ImmutableMap.<String, Object> builder().put(QUERY_ID_METRIC, queryId).build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RealizationMetrics that = (RealizationMetrics) o;

            if (!queryId.equals(that.queryId))
                return false;
            if (!suite.equals(that.suite))
                return false;
            if (!project.equals(that.project))
                return false;
            if (!realizationName.equals(that.realizationName))
                return false;
            if (!realizationType.equals(that.realizationType))
                return false;
            return modelName.equals(that.modelName);
        }

        @Override
        public int hashCode() {
            int result = queryId.hashCode();
            result = 31 * result + suite.hashCode();
            result = 31 * result + project.hashCode();
            result = 31 * result + realizationName.hashCode();
            result = 31 * result + realizationType.hashCode();
            result = 31 * result + modelName.hashCode();
            return result;
        }
    }
}
