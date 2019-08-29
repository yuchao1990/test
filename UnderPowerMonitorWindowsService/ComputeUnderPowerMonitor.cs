using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using log4net;
using UnderPowerMonitorWindowsService.DB;
using UnderPowerMonitorWindowsService.Model;

namespace UnderPowerMonitorWindowsService
{
    public class ComputeUnderPowerMonitor
    {
        private static ILog log = LogManager.GetLogger(typeof(ComputeUnderPowerMonitor));

        public static void Execute(DateTime day)
        {

            try
            {
                log.Info($"服务写入开始!开始执行时间：{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
                using (var ts = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(0, 0, 300)))
                {
                    using (BaseContext context = new BaseContext())
                    {
                        context.Database.CommandTimeout = 300;
                        //string strSql =
                        //@"SELECT pd.PSID, pd.kwh, pd.dataTime , ISNULL(loss.LosskWh, 0) AS LosskWh , pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay , ISNULL(upw.TotalDays, 0) AS TotalDays, upw.BeginTime,pm.ContractDateId,ISNULL(upw.IsUnder,0) AS IsUnder FROM T_PStationkWhDay pd LEFT JOIN T_PowerkWhManage pm ON (pd.DataTime >= pm.MonthBeginDate AND pd.DataTime <= pm.MonthEndDate AND pd.PSID = pm.PStationId) LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LosskWh FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) loss ON pd.DataTime = loss.LossDate AND pd.PSID = loss.PStationId LEFT JOIN T_UnderPowerWarning upw ON upw.WarningDate = DATEADD(day, -1, pd.DataTime) AND pd.PSID = upw.PStationId WHERE pm.MonthBeginDate IS NOT NULL AND pd.DataTime=@DataTime;";
                        // 关联电站获取 并网容量  设计容量
                        //string strSql = "SELECT pd.PSID, pd.kwh, pd.dataTime, ISNULL(loss.LosskWh, 0) AS LosskWh, pm.ForecastkWh /(DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, ISNULL(upw.TotalDays, 0) AS TotalDays, upw.BeginTime, pm.ContractDateId, ISNULL(upw.IsUnder, 0) AS IsUnder, ISNULL(upw.ContractDateId,0) AS PreContractDateId,ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= pd.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ps.DesignPower FROM T_PStationkWhDay pd LEFT JOIN T_PowerStation ps ON ps.Id = pd.PSID LEFT JOIN T_PowerkWhManage pm ON(pd.DataTime >= pm.MonthBeginDate AND pd.DataTime <= pm.MonthEndDate AND pd.PSID = pm.PStationId) LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LosskWh FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) loss ON pd.DataTime = loss.LossDate AND pd.PSID = loss.PStationId LEFT JOIN T_UnderPowerWarning upw ON upw.WarningDate = DATEADD(day, -1, pd.DataTime) AND pd.PSID = upw.PStationId WHERE pm.MonthBeginDate IS NOT NULL AND pd.DataTime = @DataTime;";
                        // 扩展并网预计发电量相关判断
                        // string strSql = "SELECT pd.PSID, pd.kwh, pd.dataTime, ISNULL(loss.LosskWh, 0) AS LosskWh, pm.ForecastkWh /(DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, ISNULL(upw.TotalDays, 0) AS TotalDays, upw.BeginTime, pm.ContractDateId, ISNULL(upw.IsUnder, 0) AS IsUnder, ISNULL(upw.ContractDateId, 0) AS PreContractDateId, ISNULL(upw.NetIsUnder, 0) AS NetIsUnder, ISNULL(upw.NetTotalDays, 0) AS NetTotalDays, upw.NetBeginTime, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= pd.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ps.DesignPower FROM T_PStationkWhDay pd LEFT JOIN T_PowerStation ps ON ps.Id = pd.PSID LEFT JOIN T_PowerkWhManage pm ON(pd.DataTime >= pm.MonthBeginDate AND pd.DataTime <= pm.MonthEndDate AND pd.PSID = pm.PStationId) LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LosskWh FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) loss ON pd.DataTime = loss.LossDate AND pd.PSID = loss.PStationId LEFT JOIN T_UnderPowerWarning upw ON upw.WarningDate = DATEADD(day, -1, pd.DataTime) AND pd.PSID = upw.PStationId WHERE pm.MonthBeginDate IS NOT NULL AND pd.DataTime = @DataTime;";
                        // 添加是否完全并网
                        string strSql = "SELECT pd.PSID, pd.kwh, pd.dataTime, ISNULL(loss.LosskWh, 0) AS LosskWh, pm.ForecastkWh /(DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, ISNULL(upw.TotalDays, 0) AS TotalDays, upw.BeginTime, pm.ContractDateId, ISNULL(upw.IsUnder, 0) AS IsUnder, ISNULL(upw.ContractDateId, 0) AS PreContractDateId, ISNULL(upw.NetIsUnder, 0) AS NetIsUnder, ISNULL(upw.NetTotalDays, 0) AS NetTotalDays, upw.NetBeginTime, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= pd.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ISNULL( ( SELECT TOP 1 GridConnectionFlag FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= pd.DataTime AND IsDel = 0 ORDER BY DataTime DESC ), 0) AS GridConnectionFlag, ps.DesignPower FROM T_PStationkWhDay pd LEFT JOIN T_PowerStation ps ON ps.Id = pd.PSID LEFT JOIN T_PowerkWhManage pm ON(pd.DataTime >= pm.MonthBeginDate AND pd.DataTime <= pm.MonthEndDate AND pd.PSID = pm.PStationId) LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LosskWh FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) loss ON pd.DataTime = loss.LossDate AND pd.PSID = loss.PStationId LEFT JOIN T_UnderPowerWarning upw ON upw.WarningDate = DATEADD(day, -1, pd.DataTime) AND pd.PSID = upw.PStationId WHERE pm.MonthBeginDate IS NOT NULL AND pd.DataTime = @DataTime;";
                        List<PowerQueryModel> retList = context.Database
                            .SqlQuery<PowerQueryModel>(strSql, new SqlParameter("@DataTime", day))
                            .ToList();
                        // 未加并网装机
                        //string strSumSql = "WITH tempByDay AS( SELECT d.PSID, d.DataTime, d.kWh, pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, d.TotalkWh, e.Loss, g.ContractKwh AS ContractPower, g.ContractBeginDate AS ContractTime, g.ContractEndDate, h.LossByFault, hh.FaultLoss, hhh.NotFaultLoss FROM T_PStationkWhDay d LEFT JOIN ( SELECT LossDate, PStationId, SUM(LosskWh) AS Loss FROM T_LosskWh GROUP BY PStationId, LossDate) e ON e.PStationId = d.PSID AND e.LossDate = d.DataTime LEFT JOIN T_ContractDate AS g ON d.PSID = g.PStationId AND d.DataTime >= g.ContractBeginDate AND d.DataTime <= g.ContractEndDate LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LossByFault FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) h ON d.PSID = H.PStationId AND H.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS FaultLoss FROM T_LosskWh WHERE FaultType = 0 GROUP BY PStationId, LossDate ) hh ON d.PSID = hh.PStationId AND hh.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS NotFaultLoss FROM T_LosskWh WHERE LossType != 12 AND FaultType = 1 GROUP BY PStationId, LossDate ) hhh ON d.PSID = hhh.PStationId AND hhh.LossDate = d.DataTime LEFT JOIN T_PowerkWhManage pm ON(d.DataTime >= pm.MonthBeginDate AND d.DataTime <= pm.MonthEndDate AND d.PSID = pm.PStationId) WHERE g.ContractEndDate >= @DataTime AND d.DataTime <= @DataTime) SELECT PSID, ContractTime, SUM(ISNULL(kWh, 0)) AS sumkWh, SUM(ISNULL(ForecastkWhDay,0)) AS sumForecastkWh, SUM(ISNULL(Loss, 0)) AS loss, SUM(ISNULL(LossByFault, 0)) AS LossByFault, SUM(ISNULL(FaultLoss, 0)) AS FaultLoss, SUM(ISNULL(NotFaultLoss, 0)) AS NotFaultLoss FROM tempByDay GROUP BY PSID, ContractTime;";
                        // 添加并网装机
                        //string strSumSql = "WITH tempByDay AS( SELECT ps.Id AS PSID, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= d.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ps.DesignPower, d.DataTime, d.kWh, pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, d.TotalkWh, e.Loss, g.ContractKwh AS ContractPower, g.ContractBeginDate AS ContractTime, g.ContractEndDate, h.LossByFault, hh.FaultLoss, hhh.NotFaultLoss FROM T_PowerStation ps LEFT JOIN M_Province b ON ps.ProvinceId = b.Id LEFT JOIN T_PStationkWhDay d ON ps.Id = d.PSID LEFT JOIN ( SELECT LossDate, PStationId, SUM(LosskWh) AS Loss FROM T_LosskWh GROUP BY PStationId, LossDate ) e ON e.PStationId = ps.Id AND e.LossDate = d.DataTime LEFT JOIN T_ContractDate AS g ON ps.id = g.PStationId AND d.DataTime >= g.ContractBeginDate AND d.DataTime <= g.ContractEndDate LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LossByFault FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) h ON ps.Id = H.PStationId AND H.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS FaultLoss FROM T_LosskWh WHERE FaultType = 0 GROUP BY PStationId, LossDate ) hh ON ps.Id = hh.PStationId AND hh.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS NotFaultLoss FROM T_LosskWh WHERE LossType != 12 AND FaultType = 1 GROUP BY PStationId, LossDate ) hhh ON ps.Id = hhh.PStationId AND hhh.LossDate = d.DataTime LEFT JOIN T_PowerkWhManage pm ON(d.DataTime >= pm.MonthBeginDate AND d.DataTime <= pm.MonthEndDate AND d.PSID = pm.PStationId) WHERE g.ContractEndDate >= @DataTime AND d.DataTime <= @DataTime) SELECT PSID, ContractTime, SUM(ISNULL(kWh, 0)) AS sumkWh, SUM(ISNULL(ForecastkWhDay, 0)) AS sumForecastkWh, SUM(ISNULL(ForecastkWhDay, 0)*InstallCapacity/DesignPower) AS sumNetForecastkWh, SUM(ISNULL(Loss, 0)) AS loss, SUM(ISNULL(LossByFault, 0)) AS LossByFault, SUM(ISNULL(FaultLoss, 0)) AS FaultLoss, SUM(ISNULL(NotFaultLoss, 0)) AS NotFaultLoss FROM tempByDay GROUP BY PSID, ContractTime;";
                        // string strSumSql = "WITH tempByDay AS( SELECT ps.Id AS PSID, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= d.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ps.DesignPower, d.DataTime, d.kWh, pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, d.TotalkWh, e.Loss, g.ContractKwh AS ContractPower, g.ContractBeginDate AS ContractTime, g.ContractEndDate, h.LossByFault, hh.FaultLoss, hhh.NotFaultLoss FROM T_PowerStation ps LEFT JOIN M_Province b ON ps.ProvinceId = b.Id LEFT JOIN T_PStationkWhDay d ON ps.Id = d.PSID LEFT JOIN ( SELECT LossDate, PStationId, SUM(LosskWh) AS Loss FROM T_LosskWh GROUP BY PStationId, LossDate ) e ON e.PStationId = ps.Id AND e.LossDate = d.DataTime LEFT JOIN T_ContractDate AS g ON ps.id = g.PStationId AND d.DataTime >= g.ContractBeginDate AND d.DataTime <= g.ContractEndDate LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LossByFault FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) h ON ps.Id = H.PStationId AND H.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS FaultLoss FROM T_LosskWh WHERE FaultType = 0 GROUP BY PStationId, LossDate ) hh ON ps.Id = hh.PStationId AND hh.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS NotFaultLoss FROM T_LosskWh WHERE LossType != 12 AND FaultType = 1 GROUP BY PStationId, LossDate ) hhh ON ps.Id = hhh.PStationId AND hhh.LossDate = d.DataTime LEFT JOIN T_PowerkWhManage pm ON(d.DataTime >= pm.MonthBeginDate AND d.DataTime <= pm.MonthEndDate AND d.PSID = pm.PStationId) WHERE g.ContractEndDate >= @DataTime AND d.DataTime <= @DataTime) SELECT PSID, ContractTime, SUM(ISNULL(kWh, 0)) AS sumkWh, SUM(ISNULL(ForecastkWhDay, 0)) AS sumForecastkWh, SUM(ISNULL(ISNULL(ForecastkWhDay, 0) * InstallCapacity / DesignPower, 0)) AS sumNetForecastkWh, SUM(ISNULL(Loss, 0)) AS loss, SUM(ISNULL(LossByFault, 0)) AS LossByFault, SUM(ISNULL(FaultLoss, 0)) AS FaultLoss, SUM(ISNULL(NotFaultLoss, 0)) AS NotFaultLoss FROM tempByDay GROUP BY PSID, ContractTime;";
                        // 添加并网装机 如果并网容量>=设计容量 算 1
                        //string strSumSql = "WITH tempByDay AS( SELECT ps.Id AS PSID, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= d.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ps.DesignPower, d.DataTime, d.kWh, pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, d.TotalkWh, e.Loss, g.ContractKwh AS ContractPower, g.ContractBeginDate AS ContractTime, g.ContractEndDate, h.LossByFault, hh.FaultLoss, hhh.NotFaultLoss FROM T_PowerStation ps LEFT JOIN M_Province b ON ps.ProvinceId = b.Id LEFT JOIN T_PStationkWhDay d ON ps.Id = d.PSID LEFT JOIN ( SELECT LossDate, PStationId, SUM(LosskWh) AS Loss FROM T_LosskWh GROUP BY PStationId, LossDate ) e ON e.PStationId = ps.Id AND e.LossDate = d.DataTime LEFT JOIN T_ContractDate AS g ON ps.id = g.PStationId AND d.DataTime >= g.ContractBeginDate AND d.DataTime <= g.ContractEndDate LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LossByFault FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) h ON ps.Id = H.PStationId AND H.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS FaultLoss FROM T_LosskWh WHERE FaultType = 0 GROUP BY PStationId, LossDate ) hh ON ps.Id = hh.PStationId AND hh.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS NotFaultLoss FROM T_LosskWh WHERE LossType != 12 AND FaultType = 1 GROUP BY PStationId, LossDate ) hhh ON ps.Id = hhh.PStationId AND hhh.LossDate = d.DataTime LEFT JOIN T_PowerkWhManage pm ON(d.DataTime >= pm.MonthBeginDate AND d.DataTime <= pm.MonthEndDate AND d.PSID = pm.PStationId) WHERE g.ContractEndDate >= @DataTime AND d.DataTime <= @DataTime) SELECT PSID, ContractTime, SUM(ISNULL(kWh, 0)) AS sumkWh, SUM(ISNULL(ForecastkWhDay, 0)) AS sumForecastkWh, ISNULL(SUM(CASE WHEN InstallCapacity >= DesignPower THEN ISNULL(ForecastkWhDay, 0) * 1 ELSE ISNULL(ForecastkWhDay, 0) * InstallCapacity / DesignPower END),0) AS sumNetForecastkWh, SUM(ISNULL(Loss, 0)) AS loss, SUM(ISNULL(LossByFault, 0)) AS LossByFault, SUM(ISNULL(FaultLoss, 0)) AS FaultLoss, SUM(ISNULL(NotFaultLoss, 0)) AS NotFaultLoss FROM tempByDay GROUP BY PSID, ContractTime;";
                        // 添加是否完全并网判断
                        string strSumSql = "WITH tempByDay AS( SELECT ps.Id AS PSID, ISNULL( ( SELECT TOP 1 InstalledCapacity FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= d.DataTime AND IsDel = 0 ORDER BY DataTime DESC), ps.InstallCapacity) AS InstallCapacity, ISNULL( ( SELECT TOP 1 GridConnectionFlag FROM T_InstalledCapacity WHERE PSID = ps.Id AND dataTime <= d.DataTime AND IsDel = 0 ORDER BY DataTime DESC ), 0) AS GridConnectionFlag, ps.DesignPower, d.DataTime, d.kWh, pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay, d.TotalkWh, e.Loss, g.ContractKwh AS ContractPower, g.ContractBeginDate AS ContractTime, g.ContractEndDate, h.LossByFault, hh.FaultLoss, hhh.NotFaultLoss FROM T_PowerStation ps LEFT JOIN M_Province b ON ps.ProvinceId = b.Id LEFT JOIN T_PStationkWhDay d ON ps.Id = d.PSID LEFT JOIN ( SELECT LossDate, PStationId, SUM(LosskWh) AS Loss FROM T_LosskWh GROUP BY PStationId, LossDate ) e ON e.PStationId = ps.Id AND e.LossDate = d.DataTime LEFT JOIN T_ContractDate AS g ON ps.id = g.PStationId AND d.DataTime >= g.ContractBeginDate AND d.DataTime <= g.ContractEndDate LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LossByFault FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) h ON ps.Id = H.PStationId AND H.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS FaultLoss FROM T_LosskWh WHERE FaultType = 0 GROUP BY PStationId, LossDate ) hh ON ps.Id = hh.PStationId AND hh.LossDate = d.DataTime LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS NotFaultLoss FROM T_LosskWh WHERE LossType != 12 AND FaultType = 1 GROUP BY PStationId, LossDate ) hhh ON ps.Id = hhh.PStationId AND hhh.LossDate = d.DataTime LEFT JOIN T_PowerkWhManage pm ON(d.DataTime >= pm.MonthBeginDate AND d.DataTime <= pm.MonthEndDate AND d.PSID = pm.PStationId) WHERE g.ContractEndDate >= @DataTime AND d.DataTime <= @DataTime) SELECT PSID, ContractTime, SUM(ISNULL(kWh, 0)) AS sumkWh, SUM(ISNULL(ForecastkWhDay, 0)) AS sumForecastkWh, ISNULL(SUM(CASE WHEN GridConnectionFlag=1 THEN ISNULL(ForecastkWhDay, 0) * 1 WHEN InstallCapacity >= DesignPower THEN ISNULL(ForecastkWhDay, 0) * 1 ELSE ISNULL(ForecastkWhDay, 0) * InstallCapacity / DesignPower END), 0) AS sumNetForecastkWh, SUM(ISNULL(Loss, 0)) AS loss, SUM(ISNULL(LossByFault, 0)) AS LossByFault, SUM(ISNULL(FaultLoss, 0)) AS FaultLoss, SUM(ISNULL(NotFaultLoss, 0)) AS NotFaultLoss FROM tempByDay GROUP BY PSID, ContractTime;";
                        List<SumPowerQueryModel> retSumList = context.Database
                            .SqlQuery<SumPowerQueryModel>(strSumSql, new SqlParameter("@DataTime", day))
                            .ToList();

                        retList.ForEach(m =>
                        {
                            T_UnderPowerWarning entity = new T_UnderPowerWarning();
                            // 告警日期
                            entity.WarningDate = day;
                            // 电站编号
                            entity.PStationId = m.PSID;
                            int IsUnder = 0;
                            if ((m.kwh + m.LosskWh) < m.ForecastkWhDay)
                            {
                                IsUnder = 1;
                            }
                            else
                            {
                                IsUnder = 0;
                            }

                            int TotalDays = 0;

                            if (IsUnder == m.IsUnder && m.PreContractDateId == m.ContractDateId)
                            {
                                TotalDays = m.TotalDays;
                            }
                            // 是否欠发
                            entity.IsUnder = IsUnder;
                            // 累计天数
                            entity.TotalDays = TotalDays + 1;
                            // 开始日期
                            entity.BeginTime = IsUnder == m.IsUnder && m.PreContractDateId == m.ContractDateId
                                ? (m.BeginTime == null ? day : m.BeginTime.Value)
                                : day;
                            // 结束日期
                            entity.EndTime = day;
                            // 当日发电量
                            entity.kWh = m.kwh;
                            // 损失发电量
                            entity.LosskWh = m.LosskWh;
                            // 计划日发电量
                            entity.ForecastkWhDay = m.ForecastkWhDay;
                            // 并网计划发电量
                            //entity.NetForecastkWhDay=m.InstallCapacity>=m.DesignPower? m.ForecastkWhDay:(m.ForecastkWhDay* m.InstallCapacity/ m.DesignPower);
                            //entity.NetForecastkWhDay = m.ForecastkWhDay * m.InstallCapacity / m.DesignPower;
                            if (m.GridConnectionFlag)
                            {
                                entity.NetForecastkWhDay = m.ForecastkWhDay;
                            }
                            else
                            {
                                entity.NetForecastkWhDay = m.InstallCapacity >= m.DesignPower ? m.ForecastkWhDay : (m.ForecastkWhDay * m.InstallCapacity / m.DesignPower);
                            }
                            #region 根据并网计划发电量判断

                            int NetIsUnder = 0;
                            if ((m.kwh + m.LosskWh) < entity.NetForecastkWhDay)
                            {
                                NetIsUnder = 1;
                            }
                            else
                            {
                                NetIsUnder = 0;
                            }

                            int NetTotalDays = 0;

                            if (NetIsUnder == m.NetIsUnder && m.PreContractDateId == m.ContractDateId)
                            {
                                NetTotalDays = m.NetTotalDays;
                            }
                            // 是否欠发
                            entity.NetIsUnder = NetIsUnder;
                            // 累计天数
                            entity.NetTotalDays = NetTotalDays + 1;
                            // 开始日期
                            entity.NetBeginTime = NetIsUnder == m.NetIsUnder && m.PreContractDateId == m.ContractDateId
                                ? (m.NetBeginTime == null ? day : m.NetBeginTime.Value)
                                : day;

                            #endregion
                            SumPowerQueryModel totalQueryModel =
                                retSumList.Where(t => t.PSID == m.PSID)?.FirstOrDefault();
                            if (totalQueryModel != null)
                            {
                                // 截至累计发电量
                                entity.TotalkWh = totalQueryModel.sumkWh;
                                // 截至累计损失
                                entity.TotalLosskWh = totalQueryModel.LossByFault;
                                // 截至累计计划发电量
                                entity.TotalForecastkWh = totalQueryModel.sumForecastkWh;
                                entity.TotalNetForecastkWh = totalQueryModel.sumNetForecastkWh;
                                // 截至故障损失
                                entity.TotalFaultLoss = totalQueryModel.FaultLoss;
                                // 截至非故障损失
                                entity.TotalNotFaultLoss = totalQueryModel.NotFaultLoss;
                                // 合同运行天数
                                entity.ContractDays = int.Parse((day - totalQueryModel.ContractTime).TotalDays.ToString()) + 1;
                            }

                            // 合同编号
                            entity.ContractDateId = m.ContractDateId;
                            entity.Remark = "服务写入";
                            entity.CreateUserId = "-1";
                            entity.CreateDate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                            entity.UpdateUserId = "-1";
                            entity.UpdateDate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                            entity.DelFlg = "0";
                            entity.UpdateVersion = 1;
                            // 刪除
                            context.Database.ExecuteSqlCommand("DELETE FROM T_UnderPowerWarning WHERE WarningDate=@DataTime And PStationId=@PStationId",
                                new SqlParameter("@DataTime", day), new SqlParameter("@PStationId", entity.PStationId));
                            context.UnderPowerWarningDbSet.Add(entity);
                            context.SaveChanges();
                        });
                    }
                    ts.Complete();
                }

                log.Info($"服务写入成功!执行完成时间：{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
            }
            catch (Exception ex)
            {
                log.Debug(ex.ToString());
            }

        }
    }
}
