# Find maximum visited in 2010

from mrjob.job import MRJob

class MRjob2(MRJob):
	def mapper1(self, _, line):
		# split data
		data = line.split('')

		# parsing data
		date = data[0].split()
		time = data[1].split()
		url = data[2].split()
		ip = data[3].split()
		
		# extract year from date
		year = date[0:4]
		month = date[5:7]
	
		# emit month and url when year is 2010
		if int(year) = 2010:
			yield (month, url), 1

	def reducder1(self, key, list_of_values):
		yield key[0], (sum(list_of_values), key[i])

	def reducder2(self, key, list_of_values):
		yield key, max(list_of_values)

	def step(self):
		return [self.mr(mapper=self.mapper1, reducer=self.reducer1), sef.mr(reducer=self.reducer2)

if __name__ == '__main__':
	MRjob2.run()
		
