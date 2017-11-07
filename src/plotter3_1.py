import matplotlib.pyplot as plt
from datetime import time, datetime
from dateutil.parser import parse 
x = []
y = []
t = []
fig = plt.figure()
rect = fig.patch
rect.set_facecolor('#31312e')
readFile = open('/Users/deepaks/Desktop/Output3_1/part-00000', 'r')
sepFile = readFile.read().strip().split('\n')
readFile.close()

for plotPair in sepFile:
 	xAndY = plotPair.split(',')
	time_string = xAndY[0][1:]
	time_string = time_string.split(" ")
	time_string = time_string[2]+"/"+time_string[1]+"/"+time_string[5]
	dt = parse(time_string)
	datetime_obj = dt.strftime('%d/%m/%Y')
	datetime_obj = datetime.strptime(datetime_obj,'%d/%m/%Y')
	t.append(datetime_obj)
	y.append(float(xAndY[1][:-1]))

ax1 = fig.add_subplot(1, 1, 1, axisbg='white')
ax1.plot(t, y, 'c', linewidth=1.1)
plt.title('CRIME RATE')
plt.xlabel('TIME')
plt.show()
	

