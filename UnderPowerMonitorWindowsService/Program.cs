using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace UnderPowerMonitorWindowsService
{
    static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        static void Main(string[] args)
        {
            //Console.WriteLine("请选择你要执行的操作——1：自动部署服务，2：安装服务，3：卸载服务，4：验证服务状态，5：退出");
            // 启动日志组件
            log4net.Config.XmlConfigurator.Configure();
            //UnderPowerMonitorJob test = new UnderPowerMonitorJob();
            //test.Execute(null);

            //double days = (DateTime.Now.Date - new DateTime(2017, 1, 1)).TotalDays;
            //DateTime day = DateTime.Now.AddDays(-days).Date;
            //for (int i = 0; i < days; i++)
            //{
            //    ComputeUnderPowerMonitor.Execute(day.AddDays(i).Date);
            //}

            //ComputeUnderPowerMonitor.Execute(DateTime.Now.AddDays(-1).Date);

            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new UnderPowerMonitorWindowsService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
