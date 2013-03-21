import os
from rabbit_helper import RabbitHelper
from cache import CacheHelper
import testcfg as cfg

# make sure logdir exists
os.system("mkdir -p "+cfg.LOGDIR)

#make sure celeybeat-schedule.db file is deleted
os.system("rm -rf celerybeat-schedule.db")

CacheHelper.cacheClean()

# kill old background processes
kill_procs=["sdkserver", "cbtop"]
for proc in kill_procs:
    os.system("ps aux | grep %s | awk '{print $2}' | xargs kill" % proc)

# cleaning up seriesly database (fast and slow created by cbtop)
os.system("curl -X DELETE http://{0}:3133/fast".format(cfg.SERIESLY_IP))
os.system("curl -X DELETE http://{0}:3133/slow".format(cfg.SERIESLY_IP))

# start sdk server
os.system("python sdkserver.py  &")

# start cbtop (ensure seriesly is running)
os.system("nohup ./cbtop -b {0} &".format(cfg.COUCHBASE_IP))
