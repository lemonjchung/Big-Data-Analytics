# Compute average visited for each page

from mrjob.job import MRJob

class MRjob3(MRJob):
	def mapper(self, _, line):
		# split line
		data = line.split('')

		# parsing data
		date = data[0].split()
		time = data[1].split()
		url = data[2].split()
		ip = data[3].split()
		visit_len = int(data[4].split())
	
		# extract year
		year = date[0:4]
		month = date[5:7]

		# emit url visit_len
		yield url, visit_len

	def reducer(self, key, list_of_values):
		count = 0
		total = 0.0
		for x in list_of_values:
			total = total+x
			count = count+1

		avgLen = ("%.2f"%(total/count))
		yield key avgLen

if __name__ == '__main__':
	MRjob3.run()
	
