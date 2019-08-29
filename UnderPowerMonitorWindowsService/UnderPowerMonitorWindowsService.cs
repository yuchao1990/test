using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace UnderPowerMonitorWindowsService
{
    public partial class UnderPowerMonitorWindowsService : ServiceBase
    {
        public UnderPowerMonitorWindowsService()
        {
            InitializeComponent();
        }
        protected override void OnStart(string[] args)
        {
            UnderPowerMonitorScheduler.GetInstance().Start();
        }

        protected override void OnStop()
        {
            if (!UnderPowerMonitorScheduler.GetInstance().IsShutdown)
                UnderPowerMonitorScheduler.GetInstance().Shutdown(false);
        }

        protected override void OnPause()
        {
            UnderPowerMonitorScheduler.GetInstance().PauseAll();
            base.OnPause();
        }

        protected override void OnContinue()
        {
            UnderPowerMonitorScheduler.GetInstance().ResumeAll();
            base.OnContinue();
        }
    }
}
