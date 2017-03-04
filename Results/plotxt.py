import matplotlib.pyplot as plt
import sys
import csv

for f in sys.argv[1:]:
	res_r = []
	res_s = []
	fi = csv.DictReader(open(f), delimiter=',',)
	for row in fi:
		res_r.append(eval(row['RATIO/OVERLAP']))
		res_s.append(eval(row['S/STRENGTH']))

	plt.xlabel('Strength')
	plt.ylabel('Overlap')
	
	plt.plot(res_s, res_r, 'o')
	plt.savefig(f+'.png')
	plt.clf()
