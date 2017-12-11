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
    leftMap = linesMap.filter(lambda line: int(line[9]) == 1)
    stayMap = linesMap.filter(lambda line:int(line[9]) == 0)

    # calculate average of each feature
    evaluationMap = linesMap.map(lambda line: float(line[1]))
    hourMap = linesMap.map(lambda line: int(line[3]))
    companyMap = linesMap.map(lambda line: int(line[4]))

    evaluationLeftMap = leftMap.map(lambda line: float(line[1]))
    hourLeftMap = leftMap.map(lambda line: int(line[3]))
    companyLeftMap = leftMap.map(lambda line: int(line[4]))

    evaluationStayMap = stayMap.map(lambda line: float(line[1]))
    hourStayMap = stayMap.map(lambda line: int(line[3]))
    companyStayMap = stayMap.map(lambda line: int(line[4]))

    print("average of last evaluation of all: ", "%.3f"%(evaluationMap.sum()/evaluationMap.count()))
    print("average of last evaluation of left: ", "%.3f"%(evaluationLeftMap.sum()/evaluationLeftMap.count()))
    print("average of last evaluation of stay: ", "%.3f"%(evaluationStayMap.sum()/evaluationStayMap.count()))
    print("average of monthly hour of all: ", "%.3f"%float((hourMap.sum())/hourMap.count()))
    print("average of monthly hour of left: ", "%.3f"%float((hourLeftMap.sum())/hourLeftMap.count()))
    print("average of monthly hour of stay: ", "%.3f"%float((hourStayMap.sum())/hourStayMap.count()))
    print("average of time spend company of all: ", "%.3f"%float((companyMap.sum())/companyMap.count()))
    print("average of time spend company of left: ", "%.3f"%float((companyLeftMap.sum())/companyLeftMap.count()))
    print("average of time spend company of stay: ", "%.3f"%float((companyStayMap.sum())/companyStayMap.count()))
    print()

    # calculate each section
    for i in range(5):

        # count the number of employees
        if i == 0:
            linesFiltered = linesMap.filter(lambda line: float(line[1]) >= 0 and float(line[1]) <= 0.2)
        else:
            linesFiltered = linesMap.filter(lambda line: float(line[1]) > i*0.2 and float(line[1]) <= i*0.2+0.2)
        evaluationNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        if i == 0:
            if evaluationNumber == 0:
                print("evaluation level leave number (", 0, " <= x <=", 0.2, "): ", 0)
                print("evaluation level total number (", 0, " <= x <=", 0.2, "): ", 0)
                print("evaluation level (", 0, " <= x <=", 0.2, "): ", 0.0)
            else:
                print("evaluation level leave number (", 0, " <= x <=", 0.2, "): ", leftNumber)
                print("evaluation level total number (", 0, " <= x <=", 0.2, "): ", evaluationNumber)
                print("evaluation level (", 0, " <= x <=", 0.2, "): ", "%.2f"%(leftNumber/float(evaluationNumber)*100)+"%")
        else:
            if evaluationNumber == 0:
                print("evaluation level leave number (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", 0)
                print("evaluation level total number (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", 0)
                print("evaluation level (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", 0.0)
            else:
                print("evaluation level leave number (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", leftNumber)
                print("evaluation level total number (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", evaluationNumber)
                print("evaluation level (", "%.1f"%(i*0.2), " < x <=", "%.1f"%(i*0.2+0.2), "): ", "%.2f"%(leftNumber/float(evaluationNumber)*100)+"%")

    print()

    for i in range(8):

        # count the number of employees
        if i == 0:
            linesFiltered = linesMap.filter(lambda line: float(line[3]) >= 0 and float(line[3]) <= 20)
        else:
            linesFiltered = linesMap.filter(lambda line: float(line[3]) > i*40 and float(line[3]) <= i*40+40)
        hourNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        if i == 0:
            if hourNumber == 0:
                print("average month hour level number (", 0, "<= x <=", 40, "): ", 0)
                print("average month hour total number(", 0, "<= x <=", 40, "): ", 0)
                print("average month hour (", 0, "<= x <=", 40, "): ", 0.0)
            else:
                print("average month hour level number (", 0, "< x <=", 40, "): ", leftNumber)
                print("average month hour total number(", 0, "< x <=", 40, "): ", hourNumber)
                print("average month hour (", 0, "<= x <=", 40, "): ", "%.2f"%(leftNumber/float(hourNumber)*100)+"%")
        else:
            if hourNumber == 0:
                print("average month hour level number (", 40*i, "< x <=", 40*i+40, "): ", 0)
                print("average month hour total number(", 40*i, "< x <=", 40*i+40, "): ", 0)
                print("average month hour (", 40*i, "< x <=", 40*i+40, "): ", 0.0)
            else:
                print("average month hour level number (", 40*i, "< x <=", 40*i+40, "): ", leftNumber)
                print("average month hour total number(", 40*i, "< x <=", 40*i+40, "): ", hourNumber)
                print("average month hour (", 40*i, "< x <=", 40*i+40, "): ", "%.2f"%(leftNumber/float(hourNumber)*100)+"%")


    print()

    for i in range(10):

        # count the number of employees
        if i == 0:
            linesFiltered = linesMap.filter(lambda line: float(line[4]) >= 0 and float(line[4]) <= 1)
        else:
            linesFiltered = linesMap.filter(lambda line: float(line[4]) > i and float(line[4]) <= i+1)
        companyNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        if i == 0:
            if companyNumber == 0:
                print("time spend company leave number (", 0, "<= x <=", 1, "): ", 0)
                print("time spend company total number (", 0, "<= x <=", 1, "): ", 0)
                print("time spend company (", 0, "<= x <=", 1, "): ", 0.0)
            else:
                print("time spend company leave number (", 0, "<= x <=", 1, "): ", leftNumber)
                print("time spend company total number (", 0, "<= x <=", 1, "): ", companyNumber)
                print("time spend company (", 0, "<= x <=", 1, "): ", "%.2f"%(leftNumber/float(companyNumber)*100)+"%")
        else:
            if companyNumber == 0:
                print("time spend company leave number (", i, "< x <=", i+1, "): ", 0)
                print("time spend company total number (", i, "< x <=", i+1, "): ", 0)
                print("time spend company (", i, "< x <=", i+1, "): ", 0.0)
            else:
                print("time spend company leave number (", i, "< x <=", i+1, "): ", leftNumber)
                print("time spend company total number (", i, "< x <=", i+1, "): ", companyNumber)
                print("time spend company (", i, "< x <=", i+1, "): ", "%.2f"%(leftNumber/float(companyNumber)*100)+"%")

    print()

    occupation = {"sales", "accounting", "hr", "technical", "management", "IT", "product_mng", "marketing", "support", "RandD"}
    for i in occupation:

        # count the number of employees
        linesFiltered = linesMap.filter(lambda line: str(line[7]) == i)
        occupationNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        if occupationNumber == 0:
            print("occupation leave number (", i, "): ", 0)
            print("occupation total number (", i, "): ", 0)
            print("occupation (", i, "): ", 0.0)
        else:
            print("occupation leave number(", i, "): ", leftNumber)
            print("occupation total number(", i, "): ", occupationNumber)
            print("occupation (", i, "): ", "%.2f"%(leftNumber/float(occupationNumber)*100)+"%")

    print()

    salary = {"low", "medium", "high"}
    for i in salary:

        # count the number of employees
        linesFiltered = linesMap.filter(lambda line: str(line[8]) == i)
        salaryNumber = linesFiltered.count()

        # count the number of ex-employees
        linesFilteredLeft = linesFiltered.filter(lambda line: int(line[9]) == 1)
        leftNumber = linesFilteredLeft.count()

        if salaryNumber == 0:
            print("salary leave number (", i, "): ", 0)
            print("salary total number(", i, "): ", 0)
            print("salary (", i, "): ", 0.0)
        else:
            print("salary (", i, "): ", leftNumber)
            print("salary (", i, "): ", salaryNumber)
            print("salary (", i, "): ", "%.2f"%(leftNumber/float(salaryNumber)*100)+"%")


if __name__ == "__main__":

    # execute main function
    main()