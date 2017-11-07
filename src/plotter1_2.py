import matplotlib.pyplot as plt
from datetime import time, datetime
from dateutil.parser import parse
from datetime import timedelta 

x = []
y = []
t = []

date = datetime(2016,9,30,0,0,0)
for i in range(31): 
    date += timedelta(days=1)
    temp = datetime.strftime(date,'%d/%m/%Y')
    t.append(datetime.strptime(temp,'%d/%m/%Y'))

fig = plt.figure()
rect = fig.patch
rect.set_facecolor('#31312e')
readFile = open('/Users/deepaks/Desktop/Output1_2/part-00000', 'r')
sepFile = readFile.readlines()
readFile.close()


for plotPair in sepFile:
	plotPair.strip()
	s = float(plotPair)
	y.append(s)

ax1 = fig.add_subplot(1, 1, 1, axisbg='white')
ax1.plot(t, y, 'c', linewidth=1.1)
plt.title('FORECASTED CRIME RATE')
plt.xlabel('FUTURE TIME')
plt.show()


