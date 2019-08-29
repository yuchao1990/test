using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Quartz;

namespace UnderPowerMonitorWindowsService
{
    public class UnderPowerMonitorJob: IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            DateTime day = DateTime.Now.AddDays(-1).Date;
            //throw new NotImplementedException();
            Thread th = new Thread(() => ComputeUnderPowerMonitor.Execute(day));
            th.Start();
        }
    }
}
