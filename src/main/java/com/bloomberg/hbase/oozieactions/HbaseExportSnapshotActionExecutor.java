package com.bloomberg.hbase.oozieactions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.snapshot.ExportSnapshot;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;

import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class HbaseExportSnapshotActionExecutor extends ActionExecutor {

    private static final String NODENAME = "hbase-export-snapshot";
    private static final String SUCCEEDED = "OK";
    private static final String FAILED = "FAIL";
    private static final String KILLED = "KILLED";
    private static final String FAILED_KILLED = "FAILED/KILLED";

    private static final String HBASE_USER = "hbase";
    private static final String HDFS_USER = "hdfs";

    private static final String HADOOP_USER = "user.name";
    private static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    private static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    private static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private static final String HADOOP_NAME_NODE = "fs.default.name";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";

    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    protected XLog LOG = XLog.getLog(getClass());

    public HbaseExportSnapshotActionExecutor() {
        super(NODENAME);
    }

   @Override
    public void initActionType() {
        super.initActionType();
        registerError(UnknownHostException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES001");
        registerError(AccessControlException.class.getName(), ActionExecutorException.ErrorType.NON_TRANSIENT,
                "JA002");
        registerError(DiskChecker.DiskOutOfSpaceException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES003");
        registerError(org.apache.hadoop.hdfs.protocol.QuotaExceededException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES004");
        registerError(org.apache.hadoop.hdfs.server.namenode.SafeModeException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES005");
        registerError(ConnectException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "  HES006");
        registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES007");
        registerError(FileNotFoundException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES008");
        registerError(IOException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES009");
    }

    static void checkForDisallowedProps(XConfiguration conf, String confName) throws ActionExecutorException {
        for (String prop : DISALLOWED_PROPERTIES) {
            if (conf.get(prop) != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                        "Property [{0}] not allowed in action [{1}] configuration", prop, confName);
            }
        }
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException
    {
        LOG = XLog.resetPrefix(LOG);
        LogUtils.setLogInfo(action, new XLog.Info());
        try {

            // Parse action configuration
            Element actionXml = XmlUtils.parseXml(action.getConf());
            Namespace ns = actionXml.getNamespace();
            String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
            String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
            String snapshotName = actionXml.getChild("snapshot-name", ns).getTextTrim();
            String destinationUri = actionXml.getChild("destination-uri", ns).getTextTrim();
            LOG.debug("Starting " + NODENAME + " for snapshot " + snapshotName);

            // Setup job configuration
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            JobConf conf = has.createJobConf(jobTracker);
            conf.set(HADOOP_USER, HBASE_USER);
            conf.set(HADOOP_JOB_TRACKER, jobTracker);
            conf.set(HADOOP_JOB_TRACKER_2, jobTracker);
            conf.set(HADOOP_YARN_RM, jobTracker);
            conf.set(HADOOP_NAME_NODE, nameNode);
            conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");
            XConfiguration actionDefaults = has.createActionDefaultConf(jobTracker, getType());
            XConfiguration.injectDefaults(actionDefaults, conf);

            Element e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                checkForDisallowedProps(inlineConf, "inline configuration");
                XConfiguration.copy(inlineConf, conf);
            }

            // Set job name
            String jobName = conf.get(HADOOP_JOB_NAME);
            if (jobName == null || jobName.isEmpty()) {
                jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}",
                        getType(), context.getWorkflow().getAppName(),
                        action.getName(), context.getWorkflow().getId());
                conf.set(HADOOP_JOB_NAME, jobName);
            }

            // Set callback
            String callback = context.getCallbackUrl("$jobStatus");
            if (conf.get("job.end.notification.url") != null) {
                LOG.warn("Overriding the action job end notification URI");
            }
            conf.set("job.end.notification.url", callback);

            String[] args = new String[5];
            args[0] = "-snapshot";
            args[1] = snapshotName;
            args[2] = "-copyTo";
            args[3] = destinationUri;
            ToolRunner.run(conf, new ExportSnapshot(), args);
            context.setExecutionData(SUCCEEDED, null);
        }
        catch (Exception ex) {
            convertException(ex);
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException
    {
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ?
        WorkflowAction.Status.OK : WorkflowAction.Status.ERROR;
        context.setEndData(status, getActionSignal(status));
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException
    {
        context.setExternalStatus(KILLED);
        context.setExecutionData(KILLED, null);
    }

    @Override
    public boolean isCompleted(String externalStatus)
    {
        return true;
    }
}
