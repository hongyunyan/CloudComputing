# import necessary libraries
from pyspark import SparkConf, SparkContext

# main function

def main():

    # configure
    conf = SparkConf().setAppName("lab 1").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # input data and split
    lines = sc.textFile('HR_comma_sep.csv')
    linesMap = lines.map(lambda line: line.split(","))
    datamap = linesMap.map(lambda line: [float(line[0]), float(line[1]), int(line[2]), int(line[3]), int(line[4]),
                                         int(line[5]), int(line[6]), str(line[7]), str(line[8]), int(line[9])])

    # calculate distance
    distanceMap = datamap.map(lambda line: [(line[0]-0.79)*(line[0]-0.79)+(line[1]-0.9)*(line[1]-0.9),line[9]])

    # sort and select the least five distance
    circleMap = distanceMap.takeOrdered(5)

    # judge leave or not
    leftNum = 0
    for i in circleMap:
        if i[1] == 1:
            ++leftNum
    if leftNum > 2:
        print ("will leave")
    else:
        print("will not leave")



if __name__ == "__main__":

    # execute main function
    main()