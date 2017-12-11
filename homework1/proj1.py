import csv

csv_reader = csv.reader(open('HR_comma_sep.csv', encoding='utf-8'))

from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

input=sc.parallelize(csv_reader);

#Q1
num = [[0]*2]*5
list=[]
i=0
while (i<5):
    result = input.filter(lambda line : float(line[0])<i*0.2+0.2 and float(line[0])>=i*0.2)
    if (i==4):
      result += input.filter(lambda line : float(line[0])==i*0.2+0.2)
    num[i][0]=result.count()
    num[i][1]=result.filter(lambda line : float(line[-1])==1).count()
    list.append(num[i][1]/num[i][0])
    i=i+1
i=0
print(list)

#Q2
from operator import add
def get(val): return val[9]

leave=input.filter(lambda line: float(line[9])==1)
stay=input.filter(lambda line: float(line[9])==0)

stayc=stay.map(lambda x:[float(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),x[7],x[8],int(x[9])]);
leavec=leave.map(lambda x:[float(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),x[7],x[8],int(x[9])]);
def mapsal(x):
    if (x[8]=="low"):
        return 1
    elif (x[8]=="medium"):
        return 2
    else:
        return 3
stayclast=stayc.map(lambda x: x[1])
staycavg=stayc.map(lambda x:x[3])
stayctime=stayc.map(lambda x: x[4])
staycocc=stayc.map(lambda x:[x[7],1])
staycsalary=stayc.map(mapsal)

leaveclast=leavec.map(lambda x:x[1])
leavecavg=leavec.map(lambda x: x[3])
leavectime=leavec.map(lambda x:x[4])
leavecocc=leavec.map(lambda x: [x[7],1])
leavecsalary=leavec.map(mapsal)

stay_last_sum = stayclast.sum()
stay_avg_sum = staycavg.sum()
stay_time_sum = stayctime.sum()
stay_sal_sum = staycsalary.sum()

leave_last_sum = leaveclast.sum()
leave_avg_sum = leavecavg.sum()
leave_time_sum = leavectime.sum()
leave_sal_sum = leavecsalary.sum()

print(stay_last_sum/leave_last_sum)
print(stay_avg_sum/leave_avg_sum)
print(stay_time_sum/leave_time_sum)
print(stay_sal_sum/leave_sal_sum)

occS=staycocc.reduceByKey(add)
occL=leavecocc.reduceByKey(add)
occ=occS.fullOuterJoin(occL)

def f(x): return x
O=occ.flatMapValues(f).collect()

i=0
while (i<20):
    print(O[i][0],":",O[i+1][1]/O[i][1])
    i=i+2

# 上面求完啦所有的平均数的对比值
# 下面对每个值分为5份来看看分布
# 第一个是lastevaluation
maxS=stayclast.max()
maxL=stayclast.max()
minS=stayclast.min()
minL=stayclast.min()
max= maxS if maxS>maxL else maxL
min= minS if minS<minL else minL
gap=(max-min)/5
part = [[0]*2]*5
i=0
listPart=[]

while (i<5):

    part[i][0]=stayclast.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    part[i][1]=leaveclast.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    if (i==4):
        part[i][0]+=stayclast.filter(lambda x:x==(min+gap*(i+1))).count()
        part[i][1]+=leaveclast.filter(lambda x:x==(min+gap*(i+1))).count()
    listPart.append(part[i][1]/(part[i][0]+part[i][1]))
    print(part[i][1]/part[i][0])
    i=i+1

print(listPart)

# 第二个是avg_month_hour
maxS=staycavg.max()
maxL=leavecavg.max()
minS=staycavg.min()
minL=leavecavg.min()
max= maxS if maxS>maxL else maxL
min= minS if minS<minL else minL
gap=(max-min)/5
part = [[0]*2]*5
i=0
listPart=[]

while (i<5):

    part[i][0]=staycavg.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    part[i][1]=leavecavg.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    if (i==4):
       part[i][0]+=staycavg.filter(lambda x:x==(min+gap*(i+1))).count()
       part[i][1]+=leavecavg.filter(lambda x:x==(min+gap*(i+1))).count()
    listPart.append(part[i][1]/(part[i][0]+part[i][1]))
    print(part[i][1]/part[i][0])
    i=i+1

print(listPart)

#第三个是time_spend_company

maxS=stayctime.max()
maxL=leavectime.max()
minS=stayctime.min()
minL=leavectime.min()
max= maxS if maxS>maxL else maxL
min= minS if minS<minL else minL
gap=(max-min)/5
part = [[0]*2]*5
i=0
listPart=[]

while (i<5):
    part[i][0]=stayctime.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    part[i][1]=leavectime.filter(lambda x:x<(min+gap*(i+1)) and x>=(min+gap*(i))).count()
    if (i==4):
        part[i][0]+=stayctime.filter(lambda x:x==(min+gap*(i+1))).count()
        part[i][1]+=leavectime.filter(lambda x:x==(min+gap*(i+1))).count()
    listPart.append(part[i][1]/(part[i][0]+part[i][1]))
    i=i+1

print(listPart)

#第四个是salary

part = [[0]*2]*3
i=0
listPart=[]

while (i<3):
    part[i][0]=staycsalary.filter(lambda x:x==1+i).count()
    part[i][1]=leavecsalary.filter(lambda x:x==1+i).count()
    listPart.append(part[i][1]/(part[i][1]+part[i][0]))
    i=i+1

print(listPart)

# Q3


datac=input.map(lambda x:[float(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),x[7],x[8],int(x[9])]);

knn=datac.map(lambda x:[(x[0]-0.79)*(x[0]-0.79)+(x[1]-0.9)*(x[1]-0.9),x[9]])
print(knn.takeOrdered(15,key=lambda x: x[0]))

#answer = 0
