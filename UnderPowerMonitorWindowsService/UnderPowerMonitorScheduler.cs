using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Quartz;
using Quartz.Impl;


namespace UnderPowerMonitorWindowsService
{
    public static class UnderPowerMonitorScheduler
    {
        private static IScheduler _scheduler;
        private static readonly object syncRoot = new object();
        public static IScheduler GetInstance()
        {
            if (_scheduler == null)
            {

                lock (syncRoot)
                {

                    if (_scheduler == null)
                    {
                        ISchedulerFactory sf = new StdSchedulerFactory();
                        _scheduler = sf.GetScheduler();
                        IJobDetail job = JobBuilder.Create<UnderPowerMonitorJob>()
                            .WithIdentity("job1", "group1")
                            .Build();
                        //"*/1 * * * * ?";
                        // 0 0 1 * * ?
                        //ITrigger trigger = TriggerBuilder.Create()
                        //    .WithIdentity("trigger1", "group1")
                        //    .StartAt(DateBuilder.FutureDate(5, IntervalUnit.Minute))// 五分钟后执行
                        //    .WithSimpleSchedule(x => x
                        //        .WithIntervalInMinutes(5)
                        //        .RepeatForever())
                        //    .Build();
                        ITrigger trigger = TriggerBuilder.Create()
                            .WithIdentity("trigger1", "group1")
                        .WithCronSchedule("0 0 10 * * ?")
                        .Build();
                        _scheduler.ScheduleJob(job, trigger);
                    }
                }
            }
            return _scheduler;
        }
    }
}
