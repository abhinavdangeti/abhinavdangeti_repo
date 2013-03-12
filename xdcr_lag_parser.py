import math
import json
from pprint import pprint
import sys

def ll(per, length):
    x = per * (length - 1)
    #print x
    if length < 10:
        return int(x)
    if (x - int(x) >= 0.5):
        #print "ceiling"
        return int(math.ceil(x))
    else:
        #print "floor"
        return int(x)

if len(sys.argv) < 2 or len(sys.argv) > 2:
	print "Usage: python my_parser.py <file.json>"
	sys.exit(1)
else:
	json_data = open(str(sys.argv[1]))
data = json.load(json_data)
#pprint(data)
lag = []
if type(data)==dict:
	for k, v in data.iteritems():
		for i in range(0, len(v)):
			lag.append(v[i]["xdcr_lag"])
elif type(data)==list:
	for i in range(0, len(data)):
		lag.append(data[i]["xdcr_lag"])
else:
	print "hmmm..."
	sys.exit(1)

#print lag
sorted_lag = sorted(lag)
#print sorted_lag
length = len(sorted_lag)
print ""
print "80th percentile:\t{0}".format(sorted_lag[ll(0.8, length)])
print "90th percentile:\t{0}".format(sorted_lag[ll(0.9, length)])
print "95th percentile:\t{0}".format(sorted_lag[ll(0.95, length)])
print "Mean:\t\t\t{0}".format(float(sum(sorted_lag))/length)
#print length
if (length + 1) % 2 == 0:
	print "Median:\t\t\t{0}".format(sorted_lag[(length) / 2])
else:
	print "Median:\t\t\t{0}".format((sorted_lag[length / 2] + sorted_lag[(length / 2) - 1]) / 2)
print ""
json_data.close()
