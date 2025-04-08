import os
import json

def checkit(name):
    result = False
    if name.startswith("mapper") and name.endswith("out.txt"):
        result = True
    elif name.startswith("reducer") and name.endswith("in.txt"):
        result = True
    elif name.startswith("reducer") and name.endswith("out.txt"):
        result = True
    return result

def checkfile(filename):
    with open(filename, "r") as infile:
        line_number = 0
        for line in infile:
            line_number = line_number+1
            if line.strip() == "":
                continue
            parts = line.split("\t")
            if len(parts) == 1:
                print(f"file: {filename}, line: {line_number}: has no tabs to separate key from value or value list.")
                return False
            if len(parts) > 2:
                print(f"file: {filename}, line: {line_number}: has more than 1 tab. That is not good.")
                return False
            try:
                json.loads(parts[0])     
            except:
                print(f"file: {filename}, line: {line_number}: key has improper format")
                printformat()
                return False
            try:
                json.loads(parts[1])     
            except:
                print(f"file: {filename}, line: {line_number}: value or value list has improper format")
                printformat()
                return False
    return True

def printformat():
    print(r"""This item should be JSON formatted. That means:
1) if it is a number it should be written as a number (no quotes) like this: 5.4
2) if it is a string, it should be written with double quotes like this: "hello"
3) if it is a list, it should be written with square brackets and the internal 
    items should be JSON formatted, like this: ["a", 3] or this ["nested", "list", [1, 2]]
    """)

def main():
    filelist = [f for f in os.listdir(".") if checkit(f)]
    for f in filelist:
        error_free = checkfile(f)
        if not error_free:
            return

if __name__ == "__main__":
    main()
