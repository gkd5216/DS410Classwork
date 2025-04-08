from mrjob.job import MRJob
from mrjob.step import MRStep

class CountryOrders(MRJob):
    """ The goal of this mapreduce job is to use data in /datasets/orders to compute,
    for every item (StockCode), the number of distinct countries that bought this item  """

    def mapper_stockcode(self, key, line):
        """ This mapper expects the following information as input:
        key: This mapper will emit the customer id as the key
        value: This mapper will emit the country or stockcode and string of it as the value 
        based on the table length
        and its goal is to yield the country and stockcode based on the customer id
        for the corresponding table """
        orders_parts = line.split("\t") #Splits dataset by tab
        if orders_parts[0] == 'InvoiceNo' or orders_parts[0] == 'CustomerID': #Skip header
            pass
        else:
            if len(orders_parts) == 2: #Customers table
                customerid, country = orders_parts[0].strip(), orders_parts[1].strip()
                yield customerid, ('country', country)
            elif len(orders_parts) == 5: #Orders table
                customerid, stockcode = orders_parts[4].strip(), orders_parts[1].strip()
                yield customerid, ('stockcode', stockcode)

    def reducer_stockcode(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer will emit the stockcode of the country as the key
        value: This reducer will emit the number of distinct countries that bought this
        item as the value
        and its goal is to yield the number of distinct countries that bought the item for every
        item (StockCode) """
        country = None
        stockcode = None #Initialize
        for value in value_list:
            if value[0] == 'country': #Country value from mapper
                country = value[1]
            elif value[0] == 'stockcode': #Stockcode value from mapper
                stockcode = value[1]
                yield stockcode, country

        if stockcode is not None:
            yield stockcode, country

    def mapper_countries(self, key, value):
        yield key, value

    def reducer_countries(self, key, value_list):
        yield key, len(set(value_list))

    def steps(self):
        """ This steps API from MrJob expects the following information as input:
        mapper: First job gets the mapper key as None and value as line; Second
        job gets the mapper key as the key output by Job 1 reducer """
        return [
                MRStep(mapper=self.mapper_stockcode, reducer=self.reducer_stockcode),
                MRStep(mapper=self.mapper_countries, reducer=self.reducer_countries)
        ]

if __name__ == '__main__':
    CountryOrders.run()

### Bugs detected and fixed ###
# 1. Output not accounting for duplicate countries; Added country and stockcode value from first mapper in first reducer
# 2. Second reducer summing values giving wrong output; Added len() and set() to second reducer value_list to account for correct length of distinct countries
# 3. First reducer not getting country and stockcode from first mapper; added strings to first mapper yield to connect to first reducer
