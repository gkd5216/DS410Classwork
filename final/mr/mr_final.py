from mrjob.job import MRJob   

class FBData(MRJob):
    """ The goal of this mapreduce job is to use the hdfs dataset /datasets/facebook
    to compute the number of appearances on the left and right for each node
    """


    def mapper(self, key, line):
        """ This mapper expects the following information as input:
        key: This mapper emits           as the key.
        value: This mapper emits           as the value.
        and its goal is to yield
        """

        fb_nodes = line.split(" ")
        for node in fb_nodes:
            left_node = node[0].strip()
            right_node = node[1].strip()
            if left_node == right_node:
                yield fb_nodes, (left_node, right_node)


    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: This combiner emits           as the key.
        value: This combiner emits           as the value.
        and its goal is to yield
        """
        for value in value_list:
            left_count = len(value[0])
            right_count = len(value[1])
        if left_count + right_count >= 3:
            yield key, (left_count, right_count)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer emits           as the key.
        value: This reducer emits           as the value.
        and its goal is to yield
        """

        for value in value_list:
            left_count = len(value[0])
            right_count = len(value[1])
        if left_count + right_count >= 3:
            yield key, (left_count, right_count)

if __name__ == '__main__':
    FBData.run() #MRJob version
