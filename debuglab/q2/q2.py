from mrjob.job import MRJob   

class USCitiesCount(MRJob):
    """ The goal of this mapreduce job is to use the Cities dataset in /datasets/cities
    to compute, for every state, the number of cities it has, the total population, and 
    the maximum number of zipcodes """

    def mapper(self, key, line): 
        """ This mapper expects the following information as input:
          key: The mapper will emit the state as the key
          value: The mapper will emit the population and length of the list of zipcodes as the value
        and its goal is to yield the state, the population in the state, and the length of the list of zip codes based on the cities dataset length """
        cities_parts = line.split("\t")
        if cities_parts[0] == 'City':
            pass
        else:
            if len(cities_parts) >= 6: #Cities dataset length
                city = cities_parts[0].strip()
                state = cities_parts[2].strip()
                population = cities_parts[4].strip() 
                zipcode = cities_parts[5].strip().split(" ")  
                yield state, (int(population), len(zipcode)) 

    def reducer(self, key, values):
        """ This reducer expects the following information as input:
        key: The reducer will emit the state as the key
        value: The reducer will emit a tuple containing the number of cities for every state, the total population for every every state, and the maximum number of zipcodes for every state as the value
        and its goal is to yield the state as the key and a tuple containing number of cities, total population, and maximum zipcodes based on an iteration through population and zipcode count values """
        
        maximum_zipcodes = 0
        total_population = 0
        num_cities = 0
        for (population, zipcode_count) in values: #tuple in mapper
            num_cities += 1 #Add sum of cities from cities dataset
            total_population += population 
            maximum_zipcodes = max(maximum_zipcodes, zipcode_count) #Track maximum zipcodes 
        yield key, (num_cities, total_population, maximum_zipcodes) #Outputs state as key from mapper and values from for loop

if __name__ == '__main__':
    USCitiesCount.run()  #MRJob version

### Bugs detected and fixed ###
# 1. Changed the class name from 'City' to 'USCitiesCount' to show meaningful class name
# 2. Added if statement to mapper to skip the header
# 3. Changed 'state' parameter in reducer to 'key' since state is classified as the key in mapper
# 4. Added if statement in else statement of mapper to filter by the cities dataset length
# 5. Changed 'p' to 'population' and 'z' to 'zipcode_count' in reducer
# 6. Added strip() to variables in mapper to rid of whitespace
# 7. Added int() to population in mapper value to get population count
# 8. Changed len(values) to 0 in num_cities variable to initialize in reducer 
# 9. Added maximum_zipcodes and total_population as variables in reducer to initialize
# 10. Added num_cities being added by 1 in reducer for loop to add sum of cities
# 11. Changed maxzips to maximum_zipcodes in reducer from initializing
# 12. Changed population to total_population in reducer from initializing
# 13. Changed z to zipcode_count to update the maximum zipcodes in a city in a state
