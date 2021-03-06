import matplotlib.pyplot as plt
import sys
import csv

START = float(sys.argv[-1])
END = float(sys.argv[-2])

for f in sys.argv[1:-2]:
	res_r = []
	res_s = []
	fi = csv.DictReader(open(f), delimiter=',',)
	for row in fi:
		res_r.append(eval(row['RATIO/OVERLAP']))
		res_s.append(eval(row['S/STRENGTH']))

	plt.ylabel('R_GC')
	res_r.reverse()
	plt.plot(res_r[START:], '+-')
	plt.savefig(f+'.r.png')
	plt.clf()

	plt.ylabel('sum(N_s*S^2)')
	res_s.reverse()
	plt.plot(res_s[START:],'rx')
	plt.savefig(f+'.s.png')
	plt.clf()
