def mapper(key, line):
    words = line.split()
    for w in words:
        yield w, len(words)
