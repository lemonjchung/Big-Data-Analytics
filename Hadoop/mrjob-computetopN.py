# top 5 visited page in each month of 2010

from mrjob.job import MRJob

class MRjob4(MRJob):
	def mapper(self, _, line):
		# split line
		data = line.split('')

		# parsing data
		date = data[0].split()
		time = data[1].split()
		url = data[2].split()
		ip = data[3].split()
		visit_len = int(data[4].split())

		# extract year and month
		year = data[0:4]
		month = data[5:7]

		# emit url when year is 2010
		if int(year) == 2010:
			yield url, 1

	def reducer1(self, key, list_of_values):
		total_count = sum(list_of_values)
		yield None, (total_count, key)

	def reducder2(self, _, list_of_values):
		n = 5
		list_of_values = sorted(list(list_of_values), reverse=True)
		return list_of_values[:n]

	def steps(self):
		return[self.mr(mapper=self.mapper, reducer=self.reducer1), self.mr(reducer=self.reducer2)]

if __name__ == '__main__':
	MRjob4.run()

