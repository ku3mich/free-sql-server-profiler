using System;
using System.Data;

namespace SqlServerTools.Data
{
    public class TraceEventArgs : EventArgs
    {
        private readonly DataTable eventsTable;

        public TraceEventArgs()
        {
        }

        public TraceEventArgs(DataTable eventsTable)
        {
            this.eventsTable = eventsTable;
        }

        public DataTable EventsTable
        {
            get
            {
                return eventsTable;
            }
        }
    }
}
