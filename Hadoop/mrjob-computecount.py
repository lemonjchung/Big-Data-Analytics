# Total count of each page is visited after 1995 year

from mrjob.job import MRJOb

class MRjob1(MRJob):
	def mapper(self, _, line):
		data = line.split('')
		
		#parsing
		date = data[0].strip()
		time = data[1].strip()
		url = data[2].strip()
		ip = data[3].strip()

		# extract year
		year = date[0:4]

		# emit url if year is greater than 1995
		if int(year) > 1995:
			yield url, 1

	def reducer(self, key, list_of_values):
		yield key, sum(list_of_values)

if __name__ == '__main__':
	MRjob1.run()



