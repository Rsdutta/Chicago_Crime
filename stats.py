import matplotlib.pyplot as plt
import numpy as np
from data import *

def getXAndYList(l, n):
    x = []
    y =[]
    count = 0
    for item in l:
        x.append(item[1])
        y.append(item[0])
        count += 1
        if count > n:
            break
    return x, y


def getXandY(l):
    xs = []
    ys = []
    for x in l:
        xs.append(x.rsplit(':',1)[0])
        ys.append(x.rsplit(':',1)[1])
    return xs, ys

def getBarChart(counts, n, title):
    x, y = getXandY(counts)
    y_pos = np.arange(n)
    plt.bar(y_pos, y[:n], align='center', alpha=0.5)
    plt.xticks(y_pos, x[:n], rotation='vertical')
    plt.ylabel('Instances')
    plt.title(title)

    plt.show()

#pops, prim_by_year, comyear, comnames, comcounts, ptcounts, topprimdesc

#most common crimes


def piePrimaryBreakdown(n):#most common crime types from top Primary types
	for key in topprimdesc:
	    desc = topprimdesc[key]
	    x, y = getXAndYList(desc, n)
	    y_pos = np.arange(n)
	    # plt.bar(y_pos, y, align='center', alpha=0.5, color=['r','m','y','b'])
	    # plt.xticks(y_pos, x, rotation='vertical')
	    # plt.ylabel('Instances')
	    print(x)
	    print(y)
	    patches, text, junk = plt.pie(y, labels=x, shadow = True, startangle=140,
	    	autopct='%1.1f%%')
	    plt.legend(patches, x, loc='best')
	    plt.tight_layout()
	    plt.axis('equal')
	    plt.title(key + ' Type Crimes')
	    plt.show()    




def crimeByArea(n):#most crime ridden communities
	y, x = getXAndYList(corrected_com_list, n)
	y_pos = np.arange(n)
	plt.bar(y_pos, y[:n], align='center', alpha=0.5)
	plt.xticks(y_pos, x[:n], rotation='vertical')
	plt.title('Average Crime Rates per Cumminty Area')
	plt.ylabel('Crimes/1000people avg per year')

	plt.show()



def barPrimaryByYear(n):#primary crimes by year in bar graphs

	fig, ax = plt.subplots()
	y_pos = np.arange(n+1)
	bar_width = 0.1
	count = 0
	colors = {'THEFT':'b', 'BATTERY':'r', 'CRIMINAL DAMAGE':'g', 'NARCOTICS':'y','ASSAULT':'m','OTHER OFFENSE':'c'
	          ,'BURGLARY':'k','DECEPTIVE PRACTICE':'k'}
	for year in range(2002,2019):   

	    x, y = getXAndYList(primyear[year], n)
	    chart_colors = []
	    for item in x:
	        chart_colors.append(colors[item])
	    plt.bar(y_pos + count*bar_width, y, label = str(year), color=chart_colors)
	    plt.ylabel('Instances')
	    plt.xticks(y_pos + count*bar_width, x, rotation='vertical')
	    count += 50
	    plt.title('Top Crime Categories ' + str(year))
	    plt.show()


def linePrimaryByYear():
	colors = {'THEFT':'b', 'BATTERY':'r', 'CRIMINAL DAMAGE':'g', 'NARCOTICS':'y','ASSAULT':'m','OTHER OFFENSE':'c'
	          ,'BURGLARY':'k','DECEPTIVE PRACTICE':'k'}
	d = {}
	# each year, add (year, occu) to primary_dict
	for year in range(2002, 2019):
		for pair in primyear[year]:
			primary_type = pair[1]
			occurences = pair[0]
			if primary_type in d.keys():
				d[primary_type].append((year, occurences))
			else:
				d[primary_type] = [(year, occurences)]

	# plot lines of each thing
	for key in d.keys():		
		year = [x[0] for x in d[key]]
		occurences = [x[1] for x in d[key]]
		plt.plot(year, occurences, label=key)
		plt.xlabel('Year')
		plt.ylabel('Occurences')
		plt.title('Crimes over Time')
		plt.legend()
	plt.show()






piePrimaryBreakdown(4)
crimeByArea(10)
barPrimaryByYear(5)
linePrimaryByYear()

