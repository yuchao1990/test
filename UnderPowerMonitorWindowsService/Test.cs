using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace UnderPowerMonitorWindowsService
{
    public class Test
    {
        private static ILog log = LogManager.GetLogger(typeof(Test));
        public static void UpdateGrid(DateTime day)
        {
           
            using (SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                connection.Open();
                //依赖是基于某一张表的,而且查询语句只能是简单查询语句,不能带top或*,同时必须指定所有者,即类似[dbo].[]
                using (SqlCommand command = new SqlCommand("SELECT pd.PSID, pd.kwh, pd.dataTime , ISNULL(loss.LosskWh, 0) AS LosskWh , pm.ForecastkWh / (DATEDIFF(DAY, pm.monthBeginDate, pm.MonthEndDate) + 1) AS ForecastkWhDay , ISNULL(upw.TotalDays, 0) AS TotalDays, upw.BeginTime,pm.ContractDateId,ISNULL(upw.IsUnder,0) AS IsUnder FROM T_PStationkWhDay pd LEFT JOIN T_PowerkWhManage pm ON (pd.DataTime >= pm.MonthBeginDate AND pd.DataTime <= pm.MonthEndDate AND pd.PSID = pm.PStationId) LEFT JOIN ( SELECT PStationId, LossDate, SUM(LosskWh) AS LosskWh FROM T_LosskWh WHERE LossType != 12 GROUP BY PStationId, LossDate ) loss ON pd.DataTime = loss.LossDate AND pd.PSID = loss.PStationId LEFT JOIN T_UnderPowerWarning upw ON upw.WarningDate = DATEADD(day, -1, pd.DataTime) AND pd.PSID = upw.PStationId WHERE pm.MonthBeginDate IS NOT NULL AND pd.DataTime=@DataTime;", connection))
                {
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter("@DataTime", day));
                    SqlDependency dependency = new SqlDependency(command);
                    dependency.OnChange += new OnChangeEventHandler(dependency_OnChange);

                    //SqlNotificationRequest notificationRequest = new SqlNotificationRequest();

                    //// Associate the notification request with the command.
                    //command.Notification = notificationRequest;
                    using (SqlDataReader sdr = command.ExecuteReader())
                    {
                        Console.WriteLine();
                        while (sdr.Read())
                        {
                            Console.WriteLine("AssyAcc:{0}\tSnum:{1}\t", sdr["PSID"].ToString(), sdr["dataTime"].ToString());
                            log.Info("AssyAcc:{0}\tSnum:{1}\t"+sdr["PSID"].ToString()+sdr["dataTime"].ToString());

                        }
                        sdr.Close();
                    }
                }
            }
            //SqlDependency.Stop(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);//传入连接字符串,启动基于数据库的监听
        }

        private static void dependency_OnChange(object sender, SqlNotificationEventArgs e)
        {
            if (e.Type == SqlNotificationType.Change) //只有数据发生变化时,才重新获取并数据
            {
                UpdateGrid(DateTime.Now.AddDays(-1).Date);
            }
        }


        public static void Initialization()
        {

            SqlDependency.Start(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);//传入连接字符串,启动基于数据库的监听
            // Create a dependency connection.
            //SqlDependency.Start(connectionString, queueName);
        }

        //void SomeMethod()
        //{
        //    // Assume connection is an open SqlConnection.

        //    // Create a new SqlCommand object.
        //    using (SqlCommand command = new SqlCommand(
        //        "SELECT ShipperID, CompanyName, Phone FROM dbo.Shippers",
        //        connection))
        //    {

        //        // Create a dependency and associate it with the SqlCommand.
        //        SqlDependency dependency = new SqlDependency(command);
        //        // Maintain the reference in a class member.

        //        // Subscribe to the SqlDependency event.
        //        dependency.OnChange += new
        //            OnChangeEventHandler(OnDependencyChange);

        //        // Execute the command.
        //        using (SqlDataReader reader = command.ExecuteReader())
        //        {
        //            // Process the DataReader.
        //        }
        //    }
        //}

        //// Handler method
        //void OnDependencyChange(object sender,
        //    SqlNotificationEventArgs e)
        //{
        //    // Handle the event (for example, invalidate this cache entry).
        //}

        //void Termination()
        //{
        //    // Release the dependency.
        //    SqlDependency.Stop(connectionString, queueName);
        //}
    }
}
