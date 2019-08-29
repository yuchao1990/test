using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnderPowerMonitorWindowsService;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            // 启动日志组件
            log4net.Config.XmlConfigurator.Configure();
            Test.Initialization();
            Test.UpdateGrid(DateTime.Now.AddDays(-1).Date);
            ComputeUnderPowerMonitor.Execute(DateTime.Now.AddDays(-1).Date);

            Console.ReadLine();
        }
    }
}
