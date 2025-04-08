from mrjob.job import MRJob   

class ZipCodesCount(MRJob):
    """ The goal of this mapreduce job is to use the hdfs dataset /datasets/cities
    to compute he zip code distribution in the data: how many cities have 0 zip codes, 
    how many have 1 zip code, etc. """

    def mapper(self, key, line):
        """ This mapper expects the following information as input:
        key: This mapper emits the zipcode count as the key.
        value: This mapper emits an integer as the value.
        and its goal is to yield the number of zipcodes and number of cities.
        """

        cities_parts = line.strip().split("\t") #Split by tab
        if cities_parts[0] == 'City': #Skip header
            pass
        else:
            if len(cities_parts) >= 6: #Cities dataset length
                zipcode = cities_parts[5].strip() #Obtains individual zipcodes from zipcodes separated by space
                if zipcode == "":
                    yield 0, 1
                else:
                    yield len(zipcode.split(" ")), 1


    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: This combiner emits the zipcode count as the key locally.
        value: This combiner emits the city count as the value locally.
        and its goal is to yield the number of zipcodes and number of cities.
        """
        yield key, sum(value_list)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer emits the zipcode count as the key globally.
        value: This reducer emits the city count as the value globally.
        and its goal is to yield the number of zipcodes and number of cities.
        """

        yield key, sum(value_list)


if __name__ == '__main__':
    ZipCodesCount.run() #MRJob version

        
