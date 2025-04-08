from mrjob.job import MRJob
from mrjob.step import MRStep

class CombinationOrders(MRJob):
    """ The goal of this mapreduce job is to use data in /datasets/orders
    to compute, for every combination of country and month, the total quantity
    of items bought in that country """

    def mapper_country(self, key, line):
        """ This mapper expects the following information as input:
        key: The mapper will emit the customerid as the key
        value: The mapper will emit an integer and the country as the 
        value for the customer table, and it will emit the month and 
        quantity as the value for the orders table
        and its goal is to yield the country, month, and quantity based 
        on the customerid for the corresponding table """
        orders_parts = line.split("\t")
        if orders_parts[0] == 'InvoiceNo' or orders_parts[0] == 'CustomerID': #Skip header
            pass
        else:
            if len(orders_parts) == 2: #Customers Table
                customerid, country = orders_parts[0].strip(), orders_parts[1].strip()
                yield customerid, (1, country)
            elif len(orders_parts) == 5: #Orders Table
                quantity = int(orders_parts[2].strip())
                customerid = orders_parts[4].strip()
                date = orders_parts[3].strip()
                datesplit = date.split('/')
                month = datesplit[0]
                yield customerid, (month, quantity)

    def reducer_country(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This mapper will emit the combination of the country and month
        as the key
        value: This mapper will emit the total quantity of items bought in the
        country as the value
        and its goal is to yield the total quantity of items for every combination
        of country and month """
        items_list = list(value_list)
        total_quantity_items = 0 #Initialize
        customerid = key
        country = None #Initialize
        current_month = None
        for value in items_list:
            if value[0] == 1: #From tuple in customers 
                country = value[1]
                break #Only one country in value list
        
        for value in items_list:
            if isinstance(value[0], str): #Month is string
                month, quantity = value[0], value[1]
                if current_month is None:
                    current_month = month #If month is empty, assign current month
                    total_quantity_items = quantity #Quantity for current month
                elif month == current_month: #Add to quantity of items if the month equals the current month
                    total_quantity_items += quantity
                else: 
                    yield (country, current_month), total_quantity_items #Prev month total quantity
                    current_month = month #Restart the month and quantity
                    total_quantity_items = quantity
        if country is not None and current_month is not None: #Prev month total quantity from prev for loop
            yield (country, month), total_quantity_items

    def mapper_quantity(self, key, value):
        yield key, value

    def reducer_quantity(self, key, value_list):
        yield key, sum(value_list)

    def steps(self):
        """ This steps API from MrJob expects the following information as input:
        mapper: First job gets the mapper key as None and value as line; Second job
        gets the mapper key as the key output by Job 1 reducer """

        return [
                MRStep(mapper=self.mapper_country, reducer=self.reducer_country),
                MRStep(mapper=self.mapper_quantity, reducer=self.reducer_quantity)
        ]

if __name__ == '__main__':
    CombinationOrders.run()

### Bugs detected and fixed ###
# 1. No month added; added invoicedate value to first mapper to retrieve the month
# 2. No quantity added; added quantity value to first mapper to retrieve the quantity of items
# 3. Nothing in index 0 of customers table value; added 1 to imitate month in orders table
# 4. No join key in first reducer; added customerid in first reducer to be join key
# 5. No output from finding instance of string being country and integer being month; replaced country with month to retrieve month and quantity from one table
# 6. No differentiation between months to combine with the country; assigned current month and corresponding quantity to each variable, added quantity when month is current, added instance to restart month and quantity
# 7. No second mapper stage and reducer stage added; added second mapper and second reducer for mrjob2 to align with each mapper and reducer defined previously
# 8. Combiner combining values from mapper and reducer; got rid of combiner to account for 2 variables in key
# 9. Multiple values in second mapper; changed values to only key and value
