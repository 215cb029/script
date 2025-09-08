import itertools
data = list(range(1, 21))
size = 5

def chunked(iterable, size):
    it = iter(iterable)
    while chunk := list(itertools.islice(it, size)):
        yield chunk

def fun():
    for chunk in chunked(data, size):
        print(chunk)

if __name__ == '__main__':
    fun()