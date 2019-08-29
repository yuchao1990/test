using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnderPowerMonitorWindowsService.Model
{
    public class PowerQueryModel
    {
        /// <summary>
        /// 电站编号
        /// </summary>
        public int PSID { get; set; }
        /// <summary>
        /// 日发电量
        /// </summary>
        public double kwh { get; set; }
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime dataTime { get; set; }
        /// <summary>
        /// 损失发电量
        /// </summary>
        public double LosskWh { get; set; }
        /// <summary>
        /// 计划日发
        /// </summary>
        public double ForecastkWhDay { get; set; }
        /// <summary>
        /// 已连续天数
        /// </summary>
        public int TotalDays { get; set; }
        /// <summary>
        /// 开始连续日期
        /// </summary>
        public DateTime? BeginTime { get; set; }

        public int ContractDateId { get; set; }

        public int IsUnder { get; set; }

        /// <summary>
        /// 上一条数据合同编号
        /// </summary>
        public int PreContractDateId { get; set; }
        public int NetIsUnder { get; set; }
        public int NetTotalDays { get; set; }
        public DateTime? NetBeginTime { get; set; }
        
        public double InstallCapacity { get; set; }
        /// <summary>
        /// 是否完全并网
        /// </summary>
        public bool GridConnectionFlag { get; set; }

        public double DesignPower { get; set; }
    }
}
