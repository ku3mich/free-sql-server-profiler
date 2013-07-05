using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using AnfiniL.SqlServerTools.Data;
using AnfiniL.SqlServerTools.Impl;
using SqlServerTools.Data;
using SqlServerTools.Exceptions;

namespace SqlServerTools.Impl
{
    public class Profiler : IProfiler
    {
        private static int TraceCounter;
        private readonly SqlConnInfo connInfo;
        private readonly object syncObj = new object();
        private readonly List<string> traceFields = new List<string>();
        private Timer getTraceTimer;
        private int lastRowNum;
        private string traceFilePath;
        private int traceId;
        private TraceStatus traceStatus;

        public Profiler()
        {
            traceId = TraceCounter++;
        }

        public Profiler(SqlConnInfo connInfo)
        {
            this.connInfo = connInfo;
            //this.connInfo.ApplicationName = "SP" + this.GetType().ToString().GetHashCode();
        }

        public string TraceFilePath
        {
            get { return traceFilePath; }
        }

        public TimeSpan RefreshInterval
        {
            get { return new TimeSpan(0, 0, 1); }
        }

        public event EventHandler<TraceEventArgs> TraceEvent;

        public int TraceId
        {
            get { return traceId; }
        }

        public TraceStatus Status
        {
            get { return traceStatus; }
        }

        public CreateTraceErrorCode Initialize(TraceOptions traceOptions, string traceFilePath, DateTime? stopTime)
        {
            return Initialize(traceOptions, traceFilePath, null, stopTime);
        }

        public CreateTraceErrorCode Initialize(TraceOptions traceOptions, string traceFilePath, int maxFileSize)
        {
            return Initialize(traceOptions, traceFilePath, maxFileSize, null);
        }

        public CreateTraceErrorCode Initialize(TraceOptions traceOptions, string traceFilePath)
        {
            return Initialize(traceOptions, traceFilePath, null, null);
        }


        public CreateTraceErrorCode Initialize(TraceOptions traceOptions, string traceFile, int? maxFileSize,
                                               DateTime? stopTime)
        {
            string file = traceFile;
            int fileIndex = 0;
            string masterFileName = GetMasterDatabaseFullPath();
            if (!string.IsNullOrEmpty(masterFileName))
            {
                while (TraceExists(masterFileName + "." + file + ".trc"))
                {
                    file = traceFile + fileIndex++;
                }

                return InitTrace(traceOptions, masterFileName + "." + file, maxFileSize, stopTime);
            }
            else
            {
                return CreateTraceErrorCode.InsufficientRights;
            }
        }

        public AddTraceEventErrorCode AddTraceEvent(TraceEvent traceEvent, params TraceField[] traceFields)
        {
            if (traceId == 0)
                throw new NotInitializedProfilerException();

            foreach (TraceField field in traceFields)
            {
                SqlCommand cmd = MsSqlUtil.NewStoredProcedure("sp_trace_setevent");
                MsSqlUtil.AddInParam(cmd, "@traceid", traceId);
                MsSqlUtil.AddInParam(cmd, "@eventid", (int)traceEvent);
                MsSqlUtil.AddInParam(cmd, "@columnid", (int)field);
                MsSqlUtil.AddInParam(cmd, "@on", true);
                MsSqlUtil.ExecuteStoredProcedure(cmd, connInfo.CreateConnectionObject());

                if (!this.traceFields.Contains(field.ToString()))
                    this.traceFields.Add(field.ToString());
            }

            return AddTraceEventErrorCode.NoError;
        }

        public AddTraceFilterErrorCode AddTraceFilter<T>(TraceField traceField, LogicalOperator logicalOp,
                                                         ComparisonOperator compOp, T value)
        {
            if (traceId == 0)
                throw new NotInitializedProfilerException();

            SqlCommand cmd = MsSqlUtil.NewStoredProcedure("sp_trace_setfilter");
            MsSqlUtil.AddInParam(cmd, "@traceid", traceId);
            MsSqlUtil.AddInParam(cmd, "@columnid", (int)traceField);
            MsSqlUtil.AddInParam(cmd, "@logical_operator", (int)logicalOp);
            MsSqlUtil.AddInParam(cmd, "@comparison_operator", (int)compOp);
            MsSqlUtil.AddInParam(cmd, "@value", value);
            return (AddTraceFilterErrorCode)MsSqlUtil.ExecuteStoredProcedure(cmd, connInfo.CreateConnectionObject());
        }

        public StatusErrorCode Start()
        {
            StatusErrorCode result = SetTraceStatus(TraceStatus.Started);
            if (result == StatusErrorCode.NoError)
            {
                getTraceTimer = new Timer(GetTraceTable, null, new TimeSpan(0, 0, 0), RefreshInterval);
            }
            return result;
        }

        public StatusErrorCode Stop()
        {
            getTraceTimer.Dispose();
            return SetTraceStatus(TraceStatus.Stopped);
        }

        public StatusErrorCode Close()
        {
            return SetTraceStatus(TraceStatus.Closed);
        }

        public IProfiler Copy()
        {
            var copy = new Profiler(connInfo) { traceFilePath = traceFilePath };
            return copy;
        }

        public CreateTraceErrorCode Initialize(TraceOptions traceOprions)
        {
            return Initialize(traceOprions, null, null, null);
        }

        private bool TraceExists(string tracePath)
        {
            SqlCommand cmd = MsSqlUtil.NewQuery("select count(*) from sys.traces where path = @tracePath");
            MsSqlUtil.AddInParam(cmd, "@tracePath", tracePath);
            var count = (int)MsSqlUtil.ExecuteScalar(cmd, connInfo.CreateConnectionObject());
            return count > 0;
        }

        private string GetMasterDatabaseFullPath()
        {
            // Bernd Linde - Using the Views in 2005 to enable also public logins to trace
            // Reference: http://msdn.microsoft.com/en-us/library/ms174397(SQL.90).aspx
            SqlCommand cmd =
                MsSqlUtil.NewQuery(
                    "use master\r\n\r\nselect top 1 rtrim([physical_name])\r\n  from sys.database_files\r\n where file_id = 1");
            string masterFullPath = string.Empty;
            masterFullPath = MsSqlUtil.ExecuteScalar(cmd, connInfo.CreateConnectionObject()) as string;
            return masterFullPath;
        }

        private CreateTraceErrorCode InitTrace(TraceOptions traceOptions, string traceFilePath, int? maxFileSize,
                                               DateTime? stopTime)
        {
            SqlCommand cmd = MsSqlUtil.NewStoredProcedure("sp_trace_create");
            SqlParameter tId = MsSqlUtil.AddOutParam(cmd, "@traceid", traceId);
            MsSqlUtil.AddInParam(cmd, "@options", (int)traceOptions);
            MsSqlUtil.AddInParam(cmd, "@tracefile", traceFilePath);
            if (maxFileSize != null)
                MsSqlUtil.AddInParam(cmd, "@maxfilesize", maxFileSize);
            if (stopTime != null)
                MsSqlUtil.AddInParam(cmd, "@stoptime", stopTime);

            int result = MsSqlUtil.ExecuteStoredProcedure(cmd, connInfo.CreateConnectionObject());
            traceId = Convert.ToInt32(tId.Value);
            this.traceFilePath = traceFilePath + ".trc";

            //Add filter to filter profiler stored procedures
            AddTraceFilter(TraceField.ApplicationName, LogicalOperator.AND, ComparisonOperator.NotEqual,
                           connInfo.ApplicationName);

            return (CreateTraceErrorCode)result;
        }

        private StatusErrorCode SetTraceStatus(TraceStatus status)
        {
            if (traceId == 0)
                throw new NotInitializedProfilerException();

            var code = StatusErrorCode.IsInvalid;

            try
            {
                SqlCommand cmd = MsSqlUtil.NewStoredProcedure("sp_trace_setstatus");
                MsSqlUtil.AddInParam(cmd, "@traceid", traceId);
                MsSqlUtil.AddInParam(cmd, "@status", status);
                code = (StatusErrorCode)MsSqlUtil.ExecuteStoredProcedure(cmd, connInfo.CreateConnectionObject());
                if (code == StatusErrorCode.NoError)
                    traceStatus = status;
            }
            catch (SqlException exc)
            {
                if (exc.Message.Contains("Could not find the requested trace"))
                {
                    return StatusErrorCode.TraceIsInvalid;
                }

                //throw;
            }

            return code;
        }

        private void OnTraceEvent(DataTable eventsTable)
        {
            if (TraceEvent != null)
                TraceEvent(this, new TraceEventArgs(eventsTable));
        }

        private void GetTraceTable(object sender)
        {
            if (traceId == 0)
                return;

            try
            {
                if (Monitor.TryEnter(syncObj))
                {
                    var cmd = new SqlCommand
                        {
                            CommandText = string.Format(
                                "select * from (select ROW_NUMBER() OVER (order by StartTime) as RowNum, {0} from fn_trace_gettable(@filename, default)) as dt where RowNum > @lastrownum",
                                traceFields.Count == 0 ? "*" : string.Join(",", traceFields.ToArray()))
                        };
                    MsSqlUtil.AddInParam(cmd, "@filename", traceFilePath);
                    MsSqlUtil.AddInParam(cmd, "@lastrownum", lastRowNum);
                    DataTable table = MsSqlUtil.ExecuteAsDataTable(cmd, connInfo.CreateConnectionObject());

                    if (table.Rows.Count > 0)
                        lastRowNum = Convert.ToInt32(table.Compute("max(RowNum)", string.Empty));

                    Monitor.Exit(syncObj);

                    OnTraceEvent(table);
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc);
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            if (traceStatus == TraceStatus.Started)
                Stop();
            if (traceStatus == TraceStatus.Stopped)
                Close();
        }

        #endregion
    }
}