using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnderPowerMonitorWindowsService.Model
{
    public class SumPowerQueryModel
    {
        public int PSID { get; set; }
        public DateTime ContractTime { get; set; }
        public double sumkWh { get; set; }
        public double sumForecastkWh { get; set; }
        public double sumNetForecastkWh { get; set; }     
        public double loss { get; set; }
        public double LossByFault { get; set; }
        public double FaultLoss { get; set; }
        public double NotFaultLoss { get; set; }
    }
}
