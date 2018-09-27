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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.util.CheckUtil.checkCondition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.OnlyPrepareEarlyAbortException;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.ServerMode;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exceptions.BigQueryException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.exceptions.UnsupportedFunctionException;
import org.apache.kylin.common.htrace.HtraceInit;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.QueryMetaProvider;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.metrics.QueryMetrics2Facade;
import org.apache.kylin.rest.metrics.QueryMetricsContext;
import org.apache.kylin.rest.metrics.QueryMetricsFacade;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.QueryRequestLimits;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Sampler;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";
    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    private final ResourceStore queryStore;

    @Autowired
    protected CacheManager cacheManager;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    private AclEvaluate aclEvaluate;

    private String restAddress = KylinConfig.getInstanceFromEnv().getServerRestAddress();

    public QueryService() {
        queryStore = ResourceStore.getStore(getConfig());
    }

    protected static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();
        DBUtils.closeQuietly(resultSet);
        DBUtils.closeQuietly(stat);
        DBUtils.closeQuietly(conn);
    }

    private static String getQueryKeyById(String creator) {
        return QUERY_STORE_PATH_PREFIX + creator;
    }

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    public List<TableMeta> getMetadata(String project) throws SQLException {
        return getMetadata(getCubeManager(), project);
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        SQLResponse ret = null;
        BadQueryDetector badQueryDetector = BadQueryDetector.getInstance(getConfig());
        try {
            final String user = SecurityContextHolder.getContext().getAuthentication().getName();
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, user);
            QueryContext.current().setSql(sqlRequest.getSql());
            markHighPriorityQueryIfNeeded();
            ret = queryWithSqlMassage(sqlRequest);
            return ret;

        } finally {
            String badReason = (ret != null && ret.isPushDown()) ? BadQueryEntry.ADJ_PUSHDOWN : null;
            badQueryDetector.queryEnd(Thread.currentThread(), badReason);
            Thread.interrupted(); //reset if interrupted
        }
    }

    private void markHighPriorityQueryIfNeeded() {
        String vipRoleName = KylinConfig.getInstanceFromEnv().getQueryVIPRole();
        if (!StringUtils.isBlank(vipRoleName) && SecurityContextHolder.getContext() //
                .getAuthentication() //
                .getAuthorities() //
                .contains(new SimpleGrantedAuthority(vipRoleName))) { //
            QueryContext.current().markHighPriorityQuery();
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        Connection conn = null;
        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil.tryPushDownNonSelectQuery(
                    sqlRequest.getProject(), sqlRequest.getSql(), conn.getSchema(), BackdoorToggles.getPrepareOnly());

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            return buildSqlResponse(true, r.getFirst(), columnMetas);

        } catch (Exception e) {
            logger.info("pushdown engine failed to finish current non-select query");
            throw e;
        } finally {
            close(null, null, conn);
        }
    }

    public void saveQuery(final String creator, final Query query) throws IOException {
        List<Query> queries = getQueries(creator);
        queries.add(query);
        Query[] queryArray = new Query[queries.size()];
        QueryRecord record = new QueryRecord(queries.toArray(queryArray));
        queryStore.putResourceWithoutCheck(getQueryKeyById(creator), record, System.currentTimeMillis(),
                QueryRecordSerializer.getInstance());
        return;
    }

    public void removeQuery(final String creator, final String id) throws IOException {
        List<Query> queries = getQueries(creator);
        Iterator<Query> queryIter = queries.iterator();

        boolean changed = false;
        while (queryIter.hasNext()) {
            Query temp = queryIter.next();
            if (temp.getId().equals(id)) {
                queryIter.remove();
                changed = true;
                break;
            }
        }

        if (!changed) {
            return;
        }
        Query[] queryArray = new Query[queries.size()];
        QueryRecord record = new QueryRecord(queries.toArray(queryArray));
        queryStore.putResourceWithoutCheck(getQueryKeyById(creator), record, System.currentTimeMillis(),
                QueryRecordSerializer.getInstance());
        return;
    }

    public List<Query> getQueries(final String creator) throws IOException {
        if (null == creator) {
            return null;
        }
        List<Query> queries = new ArrayList<Query>();
        QueryRecord record = queryStore.getResource(getQueryKeyById(creator), QueryRecord.class,
                QueryRecordSerializer.getInstance());
        if (record != null) {
            for (Query query : record.getQueries()) {
                queries.add(query);
            }
        }
        return queries;
    }

    public void logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        final List<String> realizationNames = new LinkedList<>();
        final Set<Long> cuboidIds = new HashSet<Long>();
        float duration = response.getDuration() / (float) 1000;
        boolean storageCacheUsed = response.isStorageCacheUsed();
        boolean isPushDown = response.isPushDown();

        if (!response.isHitExceptionCache() && null != OLAPContext.getThreadLocalContexts()) {
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                Cuboid cuboid = ctx.storageContext.getCuboid();
                if (cuboid != null) {
                    //Some queries do not involve cuboid, e.g. lookup table query
                    cuboidIds.add(cuboid.getId());
                }

                if (ctx.realization != null) {
                    realizationNames.add(ctx.realization.getCanonicalName());
                }

            }
        }

        int resultRowCount = 0;
        if (!response.getIsException() && response.getResults() != null) {
            resultRowCount = response.getResults().size();
        }

        String newLine = System.getProperty("line.separator");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);
        stringBuilder.append("Query Id: ").append(QueryContext.current().getQueryId()).append(newLine);
        stringBuilder.append("SQL: ").append(request.getSql()).append(newLine);
        stringBuilder.append("User: ").append(user).append(newLine);
        stringBuilder.append("Success: ").append((null == response.getExceptionMessage())).append(newLine);
        stringBuilder.append("Duration: ").append(duration).append(newLine);
        stringBuilder.append("Project: ").append(request.getProject()).append(newLine);
        stringBuilder.append("Realization Names: ").append(realizationNames).append(newLine);
        stringBuilder.append("Cuboid Ids: ").append(cuboidIds).append(newLine);
        stringBuilder.append("Total scan count: ").append(response.getTotalScanCount()).append(newLine);
        stringBuilder.append("Total scan bytes: ").append(response.getTotalScanBytes()).append(newLine);
        stringBuilder.append("Result row count: ").append(resultRowCount).append(newLine);
        stringBuilder.append("Accept Partial: ").append(request.isAcceptPartial()).append(newLine);
        stringBuilder.append("Is Partial Result: ").append(response.isPartial()).append(newLine);
        stringBuilder.append("Hit Exception Cache: ").append(response.isHitExceptionCache()).append(newLine);
        stringBuilder.append("Storage cache used: ").append(storageCacheUsed).append(newLine);
        stringBuilder.append("Is Query Push-Down: ").append(isPushDown).append(newLine);
        stringBuilder.append("Is Prepare: ").append(BackdoorToggles.getPrepareOnly()).append(newLine);
        stringBuilder.append("Is Sparder Used: ").append(response.isSparderUsed()).append(newLine);
        stringBuilder.append("Is Late Decode Enabled: ").append(response.isLateDecodeEnabled()).append(newLine);
        stringBuilder.append("Is Timeout: ").append(response.isTimeout()).append(newLine);
        stringBuilder.append("Trace URL: ").append(response.getTraceUrl()).append(newLine);
        stringBuilder.append("Message: ").append(response.getExceptionMessage()).append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);

        if (QueryMetricsContext.isStarted()) {
            QueryMetricsContext.log(stringBuilder.toString());
        }

        logger.info(stringBuilder.toString());
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        long t = System.currentTimeMillis();
        aclEvaluate.checkProjectReadPermission(sqlRequest.getProject());
        logger.info("Check query permission in " + (System.currentTimeMillis() - t) + " ms.");
        return doQueryWithCache(sqlRequest, false);
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        Message msg = MsgPicker.getMsg();
        sqlRequest.setUsername(getUserName());

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!ServerMode.isQuery(kylinConfig)) {
            throw new BadRequestException(String.format(msg.getQUERY_NOT_ALLOWED(), kylinConfig.getServerMode()));
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId());

        TraceScope scope = null;
        if (kylinConfig.isHtraceTracingEveryQuery() || BackdoorToggles.getHtraceEnabled()) {
            logger.info("Current query is under tracing");
            HtraceInit.init();
            scope = Trace.startSpan("query life cycle for " + queryContext.getQueryId(), Sampler.ALWAYS);
        }
        String traceUrl = getTraceUrl(scope);

        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            final long startTime = queryContext.getQueryStartMillis();

            SQLResponse sqlResponse = null;
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            boolean isQueryCacheEnabled = isQueryCacheEnabled(kylinConfig);
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            sql = QueryUtil.removeCommentInSql(sql);

            Pair<Boolean, String> result = TempStatementUtil.handleTempStatement(sql, kylinConfig);
            boolean isCreateTempStatement = result.getFirst();
            sql = result.getSecond();
            sqlRequest.setSql(sql);

            // try some cheap executions
            if (sqlResponse == null && isQueryInspect) {
                sqlResponse = new SQLResponse(null, null, 0, false, sqlRequest.getSql());
            }

            if (sqlResponse == null && isCreateTempStatement) {
                sqlResponse = new SQLResponse(null, null, 0, false, null);
            }

            if (sqlResponse == null && isQueryCacheEnabled) {
                sqlResponse = searchQueryInCache(sqlRequest);
                Trace.addTimelineAnnotation("query cache searched");
            }

            // real execution if required
            if (sqlResponse == null) {
                try (QueryRequestLimits limit = new QueryRequestLimits(sqlRequest.getProject())) {
                    sqlResponse = queryAndUpdateCache(sqlRequest, startTime, isQueryCacheEnabled);
                }
            } else {
                Trace.addTimelineAnnotation("response without real execution");
            }
            sqlResponse.setQueryId(QueryContext.current().getQueryId());
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            sqlResponse.setTraceUrl(traceUrl);

            String suiteId = kylinConfig.getSuiteId();
            if (suiteId != null && !suiteId.equals("")) {
                sqlResponse.setSuiteId(suiteId);
            }
            sqlResponse.setServer(restAddress);

            logQuery(sqlRequest, sqlResponse);
            try {
                recordMetric(sqlRequest, sqlResponse);
            } catch (Throwable th) {
                logger.warn("Write metric error.", th);
            }

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
            QueryContext.reset();
            if (QueryMetricsContext.isStarted()) {
                QueryMetricsContext.reset();
            }
            if (scope != null) {
                scope.close();
            }
        }
    }

    private SQLResponse queryAndUpdateCache(SQLRequest sqlRequest, long startTime, boolean queryCacheEnabled) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Message msg = MsgPicker.getMsg();

        SQLResponse sqlResponse = null;
        try {
            final boolean isSelect = QueryUtil.isSelectStatement(sqlRequest.getSql());
            if (isSelect) {
                sqlResponse = query(sqlRequest);
                Trace.addTimelineAnnotation("query almost done");
            } else if (kylinConfig.isPushDownEnabled() && kylinConfig.isPushDownUpdateEnabled()) {
                sqlResponse = update(sqlRequest);
                Trace.addTimelineAnnotation("update query almost done");
            } else {
                logger.debug("Directly return exception as the sql is unsupported, and query pushdown is disabled");
                throw new BadRequestException(msg.getNOT_SUPPORTED_SQL());
            }

            long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
            long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
            long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                    String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()),
                    String.valueOf(sqlResponse.getTotalScanCount()));
            if (checkCondition(queryCacheEnabled, "query cache is disabled") //
                    && checkCondition(!sqlResponse.getIsException(), "query has exception") //
                    && checkCondition(
                            !(sqlResponse.isPushDown()
                                    && (isSelect == false || kylinConfig.isPushdownQueryCacheEnabled() == false)),
                            "query is executed with pushdown, but it is non-select, or the cache for pushdown is disabled") //
                    && checkCondition(
                            sqlResponse.getDuration() > durationThreshold
                                    || sqlResponse.getTotalScanCount() > scanCountThreshold
                                    || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                            "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                            sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanCount(),
                            scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                    && checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(),
                            "query response is too large: {} ({})", sqlResponse.getResults().size(),
                            kylinConfig.getLargeQueryThreshold())) {
                cacheManager.getCache(SUCCESS_QUERY_CACHE).put(new Element(sqlRequest.getCacheKey(), sqlResponse));
            }
            Trace.addTimelineAnnotation("response from execution");

        } catch (Throwable e) { // calcite may throw AssertError
            logger.error("Exception while executing query", e);
            String errMsg = makeErrorMsgUserFriendly(e);

            sqlResponse = new SQLResponse(null, null, null, 0, true, errMsg, false, false);
            QueryContext queryContext = QueryContext.current();
            queryContext.setErrorCause(e);
            sqlResponse.setQueryId(queryContext.getQueryId());
            sqlResponse.setTotalScanCount(queryContext.getScannedRows());
            sqlResponse.setTotalScanBytes(queryContext.getScannedBytes());
            sqlResponse.setIsSparderUsed(queryContext.isSparderUsed());
            sqlResponse.setLateDecodeEnabled(queryContext.isLateDecodeEnabled());
            sqlResponse.setTimeout(queryContext.isTimeout());
            if (queryCacheEnabled && e.getCause() != null
                    && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
                exceptionCache.put(new Element(sqlRequest.getCacheKey(), sqlResponse));
            }
            Trace.addTimelineAnnotation("error response");
        }
        return sqlResponse;
    }

    private boolean isQueryCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") && //
                checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    protected void recordMetric(SQLRequest sqlRequest, SQLResponse sqlResponse) throws UnknownHostException {
        QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);
        QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse);
    }

    private String getTraceUrl(TraceScope scope) {
        if (scope == null) {
            return null;
        }

        String hostname = System.getProperty("zipkin.collector-hostname");
        if (StringUtils.isEmpty(hostname)) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                logger.debug("failed to get trace url due to " + e.getMessage());
                return null;
            }
        }

        String port = System.getProperty("zipkin.web-ui-port");
        if (StringUtils.isEmpty(port)) {
            port = "9411";
        }

        return "http://" + hostname + ":" + port + "/zipkin/traces/" + Long.toHexString(scope.getSpan().getTraceId());
    }

    private String getUserName() {
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (StringUtils.isEmpty(username)) {
            username = "";
        }
        return username;
    }

    public SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        SQLResponse response = null;
        Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
        Cache successCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

        Element element = null;
        if ((element = exceptionCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setHitExceptionCache(true);
        } else if ((element = successCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setStorageCacheUsed(true);
        }

        return response;
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        Connection conn = null;

        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());

            String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
            QueryContext context = QueryContext.current();
            context.setUsername(userInfo);
            context.setGroups(AclPermissionUtil.getCurrentUserGroups());
            final Collection<? extends GrantedAuthority> grantedAuthorities = SecurityContextHolder.getContext()
                    .getAuthentication().getAuthorities();
            for (GrantedAuthority grantedAuthority : grantedAuthorities) {
                userInfo += ",";
                userInfo += grantedAuthority.getAuthority();
            }

            SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
            if (null != fakeResponse) {
                logger.debug("Return fake response, is exception? " + fakeResponse.getIsException());
                return fakeResponse;
            }

            String correctedSql = QueryUtil.massageSql(sqlRequest.getSql(), sqlRequest.getProject(),
                    sqlRequest.getLimit(), sqlRequest.getOffset(), conn.getSchema());
            if (!correctedSql.equals(sqlRequest.getSql())) {
                logger.info("The corrected query: " + correctedSql);

                //CAUTION: should not change sqlRequest content!
                //sqlRequest.setSql(correctedSql);
            }
            Trace.addTimelineAnnotation("query massaged");

            // add extra parameters into olap context, like acceptPartial
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(OLAPContext.PRM_USER_AUTHEN_INFO, userInfo);
            parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
            OLAPContext.setParameters(parameters);
            // force clear the query context before a new query
            OLAPContext.clearThreadLocalContexts();

            return execute(correctedSql, sqlRequest, conn);

        } finally {
            DBUtils.closeQuietly(conn);
        }
    }

    protected List<TableMeta> getMetadata(CubeManager cubeMgr, String project) throws SQLException {

        Connection conn = null;
        ResultSet columnMeta = null;
        List<TableMeta> tableMetas = null;
        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = QueryConnection.getConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMeta>();
            Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMeta tblMeta = new TableMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMeta colmnMeta = new ColumnMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                }
            }
        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        return tableMetas;
    }

    public List<TableMetaWithType> getMetadataV2(String project) throws SQLException, IOException {
        if (null == project) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectReadPermission(project);
        }
        return QueryMetaProvider.getQueryMeta(KylinConfig.getInstanceFromEnv(), project);
    }

    protected void processStatementAttr(Statement s, SQLRequest sqlRequest) throws SQLException {
        Integer statementMaxRows = BackdoorToggles.getStatementMaxRows();
        if (statementMaxRows != null) {
            logger.info("Setting current statement's max rows to {}", statementMaxRows);
            s.setMaxRows(statementMaxRows);
        }
    }

    /**
     * @param correctedSql
     * @param sqlRequest
     * @return
     * @throws Exception
     */
    private SQLResponse execute(String correctedSql, SQLRequest sqlRequest, Connection conn) throws Exception {
        Statement stat = null;
        ResultSet resultSet = null;
        boolean isPushDown = false;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {

            // special case for prepare query.
            if (BackdoorToggles.getPrepareOnly()) {
                return getPrepareOnlySqlResponse(correctedSql, conn, isPushDown, results, columnMetas);
            }

            if (isPrepareStatementWithParams(sqlRequest)) {

                stat = conn.prepareStatement(correctedSql); // to be closed in the finally
                PreparedStatement prepared = (PreparedStatement) stat;
                processStatementAttr(prepared, sqlRequest);
                for (int i = 0; i < ((PrepareSqlRequest) sqlRequest).getParams().length; i++) {
                    setParam(prepared, i + 1, ((PrepareSqlRequest) sqlRequest).getParams()[i]);
                }
                resultSet = prepared.executeQuery();
            } else {
                stat = conn.createStatement();
                processStatementAttr(stat, sqlRequest);
                resultSet = stat.executeQuery(correctedSql);
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                        metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i),
                        metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i),
                        metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i),
                        metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i),
                        metaData.getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i),
                        metaData.isDefinitelyWritable(i)));
            }

            // fill in results
            while (resultSet.next()) {
                List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    oneRow.add((resultSet.getString(i + 1)));
                }

                results.add(oneRow);
            }

        } catch (SQLException sqlException) {
            if (hasCause(sqlException, UnsupportedFunctionException.class)) {
                QueryContext.current().switchToSparder = false;
                QueryContext.current().switchToCalcite = true;
                return execute(correctedSql, sqlRequest, conn);
            } else if (hasCause(sqlException, BigQueryException.class)) {
                QueryContext.current().switchToSparder = true;
                return execute(correctedSql, sqlRequest, conn);
            } else {
                Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
                try {
                    r = PushDownUtil.tryPushDownSelectQuery(sqlRequest.getProject(), correctedSql, conn.getSchema(),
                            sqlException, BackdoorToggles.getPrepareOnly());
                } catch (Exception e2) {
                    logger.error("pushdown engine failed current query too", e2);
                    //exception in pushdown, throw it instead of exception in calcite
                    throw e2;
                }

                if (r == null)
                    throw sqlException;

                isPushDown = true;
                results = r.getFirst();
                columnMetas = r.getSecond();
            }
        } finally {
            close(resultSet, stat, null); //conn is passed in, not my duty to close
        }

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse getPrepareOnlySqlResponse(String correctedSql, Connection conn, Boolean isPushDown,
            List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

        CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(true);

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(correctedSql);
            throw new IllegalStateException("Should have thrown OnlyPrepareEarlyAbortException");
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause != null && rootCause instanceof OnlyPrepareEarlyAbortException) {
                OnlyPrepareEarlyAbortException abortException = (OnlyPrepareEarlyAbortException) rootCause;
                CalcitePrepare.Context context = abortException.getContext();
                CalcitePrepare.ParseResult preparedResult = abortException.getPreparedResult();
                List<RelDataTypeField> fieldList = preparedResult.rowType.getFieldList();

                CalciteConnectionConfig config = context.config();

                // Fill in selected column meta
                for (int i = 0; i < fieldList.size(); ++i) {

                    RelDataTypeField field = fieldList.get(i);
                    String columnName = field.getKey();

                    if (columnName.startsWith("_KY_")) {
                        continue;
                    }
                    BasicSqlType basicSqlType = (BasicSqlType) field.getValue();

                    columnMetas.add(new SelectedColumnMeta(false, config.caseSensitive(), false, false,
                            basicSqlType.isNullable() ? 1 : 0, true, basicSqlType.getPrecision(), columnName,
                            columnName, null, null, null, basicSqlType.getPrecision(),
                            basicSqlType.getScale() < 0 ? 0 : basicSqlType.getScale(),
                            basicSqlType.getSqlTypeName().getJdbcOrdinal(), basicSqlType.getSqlTypeName().getName(),
                            true, false, false));
                }

            } else {
                throw e;
            }
        } finally {
            CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(false);
            DBUtils.closeQuietly(preparedStatement);
        }

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    private boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    private SQLResponse buildSqlResponse(Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas) {

        boolean isPartialResult = false;
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    if (cubeSb.length() > 0) {
                        cubeSb.append(",");
                    }
                    cubeSb.append(ctx.realization.getCanonicalName());
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, false, null, isPartialResult,
                isPushDown);
        response.setQueryId(QueryContext.current().getQueryId());
        response.setTotalScanCount(QueryContext.current().getScannedRows());
        response.setTotalScanBytes(QueryContext.current().getScannedBytes());
        response.setLateDecodeEnabled(QueryContext.current().isLateDecodeEnabled());
        response.setIsSparderUsed(QueryContext.current().isSparderUsed());
        return response;
    }

    /**
     * @param preparedState
     * @param param
     * @throws SQLException
     */
    private void setParam(PreparedStatement preparedState, int index, PrepareSqlRequest.StateParam param)
            throws SQLException {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }

        Rep rep = Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            preparedState.setString(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            preparedState.setInt(index, isNull ? 0 : Integer.valueOf(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            preparedState.setShort(index, isNull ? 0 : Short.valueOf(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            preparedState.setLong(index, isNull ? 0 : Long.valueOf(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            preparedState.setFloat(index, isNull ? 0 : Float.valueOf(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            preparedState.setDouble(index, isNull ? 0 : Double.valueOf(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            preparedState.setBoolean(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            preparedState.setByte(index, isNull ? 0 : Byte.valueOf(param.getValue()));
            break;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            preparedState.setDate(index, isNull ? null : java.sql.Date.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIME:
            preparedState.setTime(index, isNull ? null : Time.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIMESTAMP:
            preparedState.setTimestamp(index, isNull ? null : Timestamp.valueOf(param.getValue()));
            break;
        default:
            preparedState.setObject(index, isNull ? null : param.getValue());
        }
    }

    protected int getInt(String content) {
        try {
            return Integer.parseInt(content);
        } catch (Exception e) {
            return -1;
        }
    }

    protected short getShort(String content) {
        try {
            return Short.parseShort(content);
        } catch (Exception e) {
            return -1;
        }
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    private static class QueryRecordSerializer implements Serializer<QueryRecord> {
        private static final int MAX_WRITE_BYTE = (int) Math.pow(2, 14) - 1;

        private static final QueryRecordSerializer serializer = new QueryRecordSerializer();

        QueryRecordSerializer() {

        }

        public static QueryRecordSerializer getInstance() {
            return serializer;
        }

        @Override
        public void serialize(QueryRecord record, DataOutputStream out) throws IOException {
            String jsonStr = JsonUtil.writeValueAsString(record);
            StringBuilder sb = new StringBuilder(jsonStr);
            int count = sb.length() % MAX_WRITE_BYTE == 0 ? sb.length() / MAX_WRITE_BYTE
                    : sb.length() / MAX_WRITE_BYTE + 1;
            for (int i = 0; i < count; i++) {
                if (i == count - 1) {
                    out.writeUTF(sb.substring(i * MAX_WRITE_BYTE));
                } else {
                    out.writeUTF(sb.substring(i * MAX_WRITE_BYTE, (i + 1) * MAX_WRITE_BYTE));
                }
            }
        }

        @Override
        public QueryRecord deserialize(DataInputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (in.available() > 0) {
                sb.append(in.readUTF());
            }
            String jsonStr = sb.toString();
            return JsonUtil.readValue(jsonStr, QueryRecord.class);
        }
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> c) {
        for (; throwable != null; throwable = throwable.getCause()) {
            if (c.isInstance(throwable)) {
                return true;
            }
        }
        return false;
    }

}

@SuppressWarnings("serial")
class QueryRecord extends RootPersistentEntity {

    @JsonProperty()
    private Query[] queries;

    public QueryRecord() {

    }

    public QueryRecord(Query[] queries) {
        this.queries = queries;
    }

    public Query[] getQueries() {
        return queries;
    }

    public void setQueries(Query[] queries) {
        this.queries = queries;
    }

}
