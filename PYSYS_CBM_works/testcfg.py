#TODO: allow workers to pull this from cache

RABBITMQ_IP = '10.3.2.228'
OBJECT_CACHE_IP = "10.3.2.228"
OBJECT_CACHE_PORT = "11911"
SERIESLY_IP = "10.3.2.228"
COUCHBASE_IP = '10.1.3.235'
COUCHBASE_PORT = '8091'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "password"
SSH_USER = "root"
SSH_PASSWORD = "couchbase"
WORKERS = ['10.3.2.228']
# valid configs ["kv","query","admin","stats"] or ["all"]
WORKER_CONFIGS = ["all"]
CB_CLUSTER_TAG = "self"
ATOP_LOG_FILE = "/tmp/atop-node.log"
LOGDIR="logs"  # relative to current dir

#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
BACKUP_NODE_SSH_PWD = "password"
