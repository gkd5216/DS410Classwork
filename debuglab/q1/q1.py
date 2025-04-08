from mrjob.job import MRJob   

class CustomerOrders(MRJob): 
    """ The goal of this mapreduce job is to use the data files in /dataset/orders
    to compute, for every customer id, the country and total quantity of items bought by 
    the customer. So, when the job is finished, each line of the output should contain
    customer id (key) and [country, totalquantity] as the value  """

    def mapper(self, key, line):
        """ This mapper expects the following information as input:
          key: The mapper will emit the customerid as the key
          value: The mapper will emit the country as the value for the customers table and emit the quantity as the value for the orders table        
          and its goal is to yield the country or quantity based on the customerid from the corresponding table"""
        orders_parts = line.split("\t") #split data by tab
        if orders_parts[0] == 'InvoiceNo' or orders_parts[0] == 'CustomerID': #Skip header
            pass
        else:
            if len(orders_parts) == 2: #Customers
                customerid, country = orders_parts[0].strip(), orders_parts[1].strip()
                yield customerid, country #Gather customer for joining 
            elif len(orders_parts) == 5: #Orders
                customerid, quantity = orders_parts[4].strip(), int(orders_parts[2].strip())
                yield customerid, quantity  #Gather order for joining
    def reducer(self, key, values):
        """ This reducer expects the following information as input:
        key: The reducer will emit the customerid as the key
        value: The reducer will emit the country and total quantity of items as the value
        and its goal is to sum up the total quantity of items bought by the customer
        and determine the country for each customer based on the value type """
        items_list = list(values) #Because we need 2 iterations
        total_quantity = 0 
        country = None #Initialize
        for value in items_list: 
            if isinstance(value, str): #Must be country
                country = value
                break #Only need to find the country once
        for value in items_list:
            if isinstance(value, int): #Must be quantity
                total_quantity += value
        if country is not None:
            yield key, (country, total_quantity)

if __name__ == '__main__':
    CustomerOrders.run()  

### Bugs detected and fixed ###
# 1. Added the 'key' variable that was forgotten in definition of reducer
# 2. Changed the class name from 'BadClassName' to 'CustomerOrders' for a meaningful class name
# 3. Added all of the data to 'line.split("\t")' forgotten in splitting data in the mapper
# 4. Changed run class to 'CustomerOrders' to align with class name
# 5. Added if statement in the mapper to skip the header
# 6. Changed mapper yield statement to key, value statements based on customers and orders table length
# 7. Changed country value from 'values[0]' to None in reducer to initialize variable
# 8. Changed total_quantity value from 'sum(values[1:])' to 0 in reducer to add quantity count
# 9. Changed yield statement in reducer from 'customerid' to 'key' since the key is defined as the customerid in mapper
# 10. Added if statements in the else statement of the mapper to distinguish between the customers table and orders table
# 11. Changed the value of customerid and country to individual index number in the split data to gather data
# 12. Changed values in reducer to list for 2 iterations of getting country and total quantity of items
# 13. Added strip() to variables in mapper to rid of whitespace
# 14. Added for loop through list of values to get instances of strings and integers for the final code output
