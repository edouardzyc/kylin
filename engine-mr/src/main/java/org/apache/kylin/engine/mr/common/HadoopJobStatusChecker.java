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

package org.apache.kylin.engine.mr.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HadoopJobStatusChecker {
    protected static final Logger logger = LoggerFactory.getLogger(HadoopJobStatusChecker.class);

    public static class HadoopStatusGetter {

        private final String mrJobId;
        private final String yarnUrl;

        public HadoopStatusGetter(String yarnUrl, String mrJobId) {
            this.yarnUrl = yarnUrl;
            this.mrJobId = mrJobId;
        }

        public Pair<RMAppState, FinalApplicationStatus> get() throws IOException {
            String applicationId = mrJobId.replace("job", "application");
            String url = yarnUrl.replace("${job_id}", applicationId);
            String response = getHttpResponse(url);
//            logger.debug("Hadoop job " + mrJobId + " status : " + response);
            JsonNode root = new ObjectMapper().readTree(response);
            RMAppState state = RMAppState.valueOf(root.findValue("state").textValue());
            FinalApplicationStatus finalStatus = FinalApplicationStatus.valueOf(root.findValue("finalStatus").textValue());
            return Pair.of(state, finalStatus);
        }

        private String getHttpResponse(String url) throws IOException {
            HttpClient client = new HttpClient();

            String response = null;
            while (response == null) { // follow redirects via 'refresh'
                if (url.startsWith("https://")) {
                    registerEasyHttps();
                }
                if (url.contains("anonymous=true") == false) {
                    url += url.contains("?") ? "&" : "?";
                    url += "anonymous=true";
                }

                HttpMethod get = new GetMethod(url);
                get.addRequestHeader("accept", "application/json");

                try {
                    client.executeMethod(get);

                    String redirect = null;
                    Header h = get.getResponseHeader("Location");
                    if (h != null) {
                        redirect = h.getValue();
                        if (isValidURL(redirect) == false) {
                            logger.info("Get invalid redirect url, skip it: " + redirect);
                            Thread.sleep(1000L);
                            continue;
                        }
                    } else {
                        h = get.getResponseHeader("Refresh");
                        if (h != null) {
                            String s = h.getValue();
                            int cut = s.indexOf("url=");
                            if (cut >= 0) {
                                redirect = s.substring(cut + 4);

                                if (isValidURL(redirect) == false) {
                                    logger.info("Get invalid redirect url, skip it: " + redirect);
                                    Thread.sleep(1000L);
                                    continue;
                                }
                            }
                        }
                    }

                    if (redirect == null) {
                        response = get.getResponseBodyAsString();
//                        logger.debug("Job " + mrJobId + " get status check result.\n");
                    } else {
                        url = redirect;
//                        logger.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                    }
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                } finally {
                    get.releaseConnection();
                }
            }

            return response;
        }

        private Protocol EASY_HTTPS = null;

        private void registerEasyHttps() {
            // by pass all https issue
            if (EASY_HTTPS == null) {
                EASY_HTTPS = new Protocol("https", (ProtocolSocketFactory) new DefaultSslProtocolSocketFactory(), 443);
                Protocol.registerProtocol("https", EASY_HTTPS);
            }
        }

        private boolean isValidURL(String value) {
            if (StringUtils.isNotEmpty(value)) {
                java.net.URL url;
                try {
                    url = new java.net.URL(value);
                } catch (MalformedURLException var5) {
                    return false;
                }

                return StringUtils.isNotEmpty(url.getProtocol()) && StringUtils.isNotEmpty(url.getHost());
            }

            return false;
        }
    }

    private static String getRestStatusCheckUrl(Job job, KylinConfig config) {
        final String yarnStatusCheckUrl = config.getYarnStatusCheckUrl();
        if (yarnStatusCheckUrl != null) {
            return yarnStatusCheckUrl;
        } else {
//            logger.info("kylin.job.yarn.app.rest.check.status.url" + " is not set, read from job configuration");
        }
        String rmWebHost = HAUtil.getConfValueForRMInstance(YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, job.getConfiguration());
        if (HAUtil.isHAEnabled(job.getConfiguration())) {
            YarnConfiguration conf = new YarnConfiguration(job.getConfiguration());
            String active = RMHAUtils.findActiveRMHAId(conf);
            rmWebHost = HAUtil.getConfValueForRMInstance(HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_ADDRESS, active), YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, conf);
        }
        if (StringUtils.isEmpty(rmWebHost)) {
            return null;
        }
        if (rmWebHost.startsWith("http://") || rmWebHost.startsWith("https://")) {
            //do nothing
        } else {
            rmWebHost = "http://" + rmWebHost;
        }
//        logger.info("yarn.resourcemanager.webapp.address:" + rmWebHost);
        return rmWebHost + "/ws/v1/cluster/apps/${job_id}?anonymous=true";
    }

    public static JobStepStatusEnum checkStatus(Job job, StringBuilder output) {
        if (null == job.getJobID()) {
            logger.debug("Skip status check with empty job id..");
            return JobStepStatusEnum.WAITING;
        }
        JobStepStatusEnum status = null;
        String mrJobID = job.getJobID().toString();

        try {
            final Pair<RMAppState, FinalApplicationStatus> result = new HadoopStatusGetter(getRestStatusCheckUrl(job, KylinConfig.getInstanceFromEnv()), mrJobID).get();
            logger.debug("State of Hadoop job: " + mrJobID + ":" + result.getLeft() + "-" + result.getRight());
            output.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date()) + " - State of Hadoop job: " + mrJobID + ":" + result.getLeft() + " - " + result.getRight() + "\n");

            switch (result.getRight()) {
                case SUCCEEDED:
                    status = JobStepStatusEnum.FINISHED;
                    break;
                case FAILED:
                    status = JobStepStatusEnum.ERROR;
                    break;
                case KILLED:
                    status = JobStepStatusEnum.KILLED;
                    break;
                case UNDEFINED:
                    switch (result.getLeft()) {
                        case NEW:
                        case NEW_SAVING:
                        case SUBMITTED:
                        case ACCEPTED:
                            status = JobStepStatusEnum.WAITING;
                            break;
                        case RUNNING:
                            status = JobStepStatusEnum.RUNNING;
                            break;
                        case FINAL_SAVING:
                        case FINISHING:
                        case FINISHED:
                        case FAILED:
                        case KILLING:
                        case KILLED:
                        default:
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            logger.error("error check status", e);
            output.append("Exception: " + e.getLocalizedMessage() + "\n");
            status = JobStepStatusEnum.ERROR;
        }

        if (status == null)
            throw new IllegalStateException();

        return status;
    }

}
