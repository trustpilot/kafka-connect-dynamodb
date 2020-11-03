package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.cloudwatch.waiters.AmazonCloudWatchWaiters;

/**
 * This class allows KCL to be started without CloudWatch connectivity.
 */
public class KclNoopCloudWatch implements AmazonCloudWatch {
    @Deprecated
    @Override
    public void setEndpoint(String s) {

    }

    @Deprecated
    @Override
    public void setRegion(Region region) {

    }


    @Override
    public DeleteAlarmsResult deleteAlarms(DeleteAlarmsRequest deleteAlarmsRequest) {
        return null;
    }

    @Override
    public DeleteAnomalyDetectorResult deleteAnomalyDetector(DeleteAnomalyDetectorRequest deleteAnomalyDetectorRequest) {
        return null;
    }

    @Override
    public DescribeAlarmHistoryResult describeAlarmHistory(DescribeAlarmHistoryRequest describeAlarmHistoryRequest) {
        return null;
    }

    @Override
    public DescribeAlarmHistoryResult describeAlarmHistory() {
        return null;
    }

    @Override
    public DescribeAlarmsResult describeAlarms(DescribeAlarmsRequest describeAlarmsRequest) {
        return null;
    }

    @Override
    public DescribeAlarmsResult describeAlarms() {
        return null;
    }

    @Override
    public DescribeAlarmsForMetricResult describeAlarmsForMetric(DescribeAlarmsForMetricRequest describeAlarmsForMetricRequest) {
        return null;
    }

    @Override
    public DescribeAnomalyDetectorsResult describeAnomalyDetectors(DescribeAnomalyDetectorsRequest describeAnomalyDetectorsRequest) {
        return null;
    }

    @Override
    public DescribeInsightRulesResult describeInsightRules(DescribeInsightRulesRequest describeInsightRulesRequest) {
        return null;
    }

    @Override
    public DisableAlarmActionsResult disableAlarmActions(DisableAlarmActionsRequest disableAlarmActionsRequest) {
        return null;
    }

    @Override
    public DisableInsightRulesResult disableInsightRules(DisableInsightRulesRequest disableInsightRulesRequest) {
        return null;
    }

    @Override
    public EnableAlarmActionsResult enableAlarmActions(EnableAlarmActionsRequest enableAlarmActionsRequest) {
        return null;
    }

    @Override
    public EnableInsightRulesResult enableInsightRules(EnableInsightRulesRequest enableInsightRulesRequest) {
        return null;
    }

    @Override
    public GetMetricStatisticsResult getMetricStatistics(GetMetricStatisticsRequest getMetricStatisticsRequest) {
        return null;
    }

    @Override
    public GetMetricWidgetImageResult getMetricWidgetImage(GetMetricWidgetImageRequest getMetricWidgetImageRequest) {
        return null;
    }

    @Override
    public ListMetricsResult listMetrics(ListMetricsRequest listMetricsRequest) {
        return null;
    }

    @Override
    public ListMetricsResult listMetrics() {
        return null;
    }

    @Override
    public ListTagsForResourceResult listTagsForResource(ListTagsForResourceRequest listTagsForResourceRequest) {
        return null;
    }

    @Override
    public PutAnomalyDetectorResult putAnomalyDetector(PutAnomalyDetectorRequest putAnomalyDetectorRequest) {
        return null;
    }

    @Override
    public PutMetricAlarmResult putMetricAlarm(PutMetricAlarmRequest putMetricAlarmRequest) {
        return null;
    }

    @Override
    public PutMetricDataResult putMetricData(PutMetricDataRequest putMetricDataRequest) {
        return null;
    }

    @Override
    public SetAlarmStateResult setAlarmState(SetAlarmStateRequest setAlarmStateRequest) {
        return null;
    }

    @Override
    public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
        return null;
    }

    @Override
    public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        return null;
    }

    @Override
    public AmazonCloudWatchWaiters waiters() {
        return null;
    }

    @Override
    public DeleteDashboardsResult deleteDashboards(DeleteDashboardsRequest deleteDashboardsRequest) {
        return null;
    }

    @Override
    public DeleteInsightRulesResult deleteInsightRules(DeleteInsightRulesRequest deleteInsightRulesRequest) {
        return null;
    }

    @Override
    public GetDashboardResult getDashboard(GetDashboardRequest getDashboardRequest) {
        return null;
    }

    @Override
    public GetInsightRuleReportResult getInsightRuleReport(GetInsightRuleReportRequest getInsightRuleReportRequest) {
        return null;
    }

    @Override
    public GetMetricDataResult getMetricData(GetMetricDataRequest getMetricDataRequest) {
        return null;
    }

    @Override
    public ListDashboardsResult listDashboards(ListDashboardsRequest listDashboardsRequest) {
        return null;
    }

    @Override
    public PutDashboardResult putDashboard(PutDashboardRequest putDashboardRequest) {
        return null;
    }

    @Override
    public PutInsightRuleResult putInsightRule(PutInsightRuleRequest putInsightRuleRequest) {
        return null;
    }
}
