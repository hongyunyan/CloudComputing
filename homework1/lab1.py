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

    # calculate each section
    for i in range(5):

        # count the number of employees
        linesFiltered = linesMap.filter(lambda line: float(line[0]) > i*0.2 and float(line[0]) <= i*0.2+0.2)
        satisfactionNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda  line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        print("satisfaction level (", "%.1f"%(i*0.2), "< x <=", "%.1f"%(i*0.2+0.2), "): ", "%.2f"%(leftNumber/float(satisfactionNumber)*100)+"%")

if __name__ == "__main__":

    # execute main function
    main()