The reason the Database Engine Health metrics aren't displaying is that the backend simulator process is still running an older, cached version of the code from before we added those new metrics (LDF Size, PLE, TempDB, etc.). The frontend expects them, but the old backend is only sending { cpu, io } so they end up blank.

Because the simulator runs in a hidden window via your run_simulation.ps1 script, it didn't automatically reload when we made those changes to db.ts.

To fix this, you just need to completely restart the simulator:

Close the PowerShell window where run_simulation.ps1 is running.
In a new PowerShell window, run Stop-Process -Name "node" -Force to clean up the hidden stale processes.
Start the run_simulation.ps1 script again.
Once the backend boots up with the fresh code, those metrics will immediately start streaming and displaying correctly! Let me know when you've restarted it.