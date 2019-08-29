%~dp0installutil.exe %~dp0UnderPowerMonitorWindowsService.exe
Net Start 欠发电量监控服务
sc config 欠发电量监控服务 start= auto