from mrjob.job import MRJob
from mrjob.step import MRStep

class CountryOrders(MRJob):
    """ The goal of this mapreduce job is to use data in /datasets/orders to compute how
    many orders have 1 part, how many orders have 2 parts, etc. """

    def mapper_invoice(self, key, line):
        """ This mapper expects the following information as input:
        key: The mapper will emit the invoice number as the key
        value: The mapper will emit an integer as a placeholder for the value
        and its goal is to yield the amount of orders and how many parts the order has """

        orders_parts = line.split("\t")
        if orders_parts[0] == 'InvoiceNo' or orders_parts[0] == 'CustomerID': #Skip header
            pass
        else:
            if len(orders_parts) == 5: #Customers Table
                invoiceno = orders_parts[0].strip()
                yield invoiceno, 1

    def reducer_invoice(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer will emit the invoice number as the key
        value: This reducer will emit the number of order parts as the value
        and its goal is to yield the number of orders and how many parts the order has """

        parts_order = sum(value_list) #Sum of number of order parts
        yield parts_order, 1

    def mapper_parts(self, key, value):
        yield key, value

    def reducer_parts(self, key, value_list):
        yield key, sum(value_list) #Sum the number of orders
        
    def steps(self):
        """ This steps API from MrJob expects the following information as input:
        mapper: First job gets the mapper key as None and value as line; Second
        job gets the mapper key as the key output by Job 1 reducer """
        return [
                MRStep(mapper=self.mapper_invoice, reducer=self.reducer_invoice),
                MRStep(mapper=self.mapper_parts, reducer=self.reducer_parts)
        ]
if __name__ == '__main__':
    CountryOrders.run()

### Bugs detected and fixed ###
# 1. Skip header line missing; added indices to establish skipping header 'Invoice Number' or 'Customer ID'
# 2. No value established in first mapper; added 1 as value when yielding invoice number as key
# 3. Number of order parts not adding up; added sum in first reducer to add up the number of order parts
