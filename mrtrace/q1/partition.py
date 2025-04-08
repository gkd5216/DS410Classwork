import hashlib

def what_would_partitioner_do(idname, key, numreducers):
    """ This function tells us what the partitioner would do in our 
    hypothetical setup for tracing through a mapreduce program.

    Suppose your psu id is abc1234, the number of reducers is 4 and you
    want to know which reducers will be assigned to the keys "hello", "world", and 3
    Then you would call this function like this:
    
    what_would_partitioner_do("abc1234", "hi", 4) # returns 1 (i.e. it will go to reducer 1)
    what_would_partitioner_do("abc1234", "worlds", 4)  #returns 1
    what_would_partitioner_do("abc1234", 5, 4)  # returns 3
    what_would_partitioner_do("abc1234", "5", 4)  # returns 2
    what_would_partitioner_do("abc1234", ["a", 7], 4) # returns 2

    IMPORTANT: be super careful about the types: 5 and "5" are different things (the first is a string, the second is a number). '["a", 7]' and ["a", 7] are different things (the first is a string, the second is a list)

    """
    result = makehash(f"{idname}|{type(key)}|{key}") % numreducers
    return result


def makehash(k):
    thehash = int(hashlib.sha1(bytes(str(k),'utf-8')).hexdigest(), 16) 
    return thehash
    
