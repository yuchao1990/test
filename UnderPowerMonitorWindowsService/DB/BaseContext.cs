using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnderPowerMonitorWindowsService.Model;

namespace UnderPowerMonitorWindowsService.DB
{
    public class BaseContext : DbContext
    {
        public BaseContext()
            : base("Name=DefaultConnection")
        {
            Database.SetInitializer<BaseContext>(null);
        }
        public DbSet<T_UnderPowerWarning> UnderPowerWarningDbSet { get; set; }
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    }
}
