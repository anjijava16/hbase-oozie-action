package com.bloomberg.hbase.oozieactions;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

public class HbaseImportSnapshotActionExecutor extends ActionExecutor {

    private static final String NODENAME = "hbase-import-snapshot";
    private static final String SUCCEEDED = "OK";
    private static final String FAILED = "FAIL";
    private static final String KILLED = "KILLED";

    protected XLog LOG = XLog.getLog(getClass());

    public HbaseImportSnapshotActionExecutor() {
        super(NODENAME);
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException
    {
        context.setExecutionData(SUCCEEDED, null);
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
