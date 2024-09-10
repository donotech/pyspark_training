import os
import sys
from functools import reduce

class Participants:
    def __init__(self, name, age, mobile):
        self.name = name
        self.mobile = mobile
        self.age = age
        self.is_adult = (age >= 18)
        self.city = None

    def print_values(self):
        return "name: " + self.name + " is_adult " + str(self.is_adult) + " city " + str(self.city)

    def set_new_attribute(self, city):
        self.city = city


def hello_world():
    return "hello world"


def some_program():
    hello_world()
    print("bye world")
    print('single quote')
    v: int = 1  # v -> memory location -> points to actual data
    s: str = "one"
    print(v)
    v = "three"
    print(v)
    print(__name__)


def some_foo(x):
    return x + 2


def find_max(x: list):
    # return the maximum element in the list
    pass

find_max([10,4,9,100,3,99]) # return 100

print('first line')

if __name__ == '__main__':
    print('protected line')
    print("python_basics")
    print(sys.argv)
    os.environ['SOME_PATH'] = 'C:\\training\\python'

    ltest = [1, 2, 3, 4, 5, 6, 7]
    ltest_odd_squares = map(lambda x: x * x, filter(lambda x: x % 2 != 0, ltest))
    ltest_odd_squares_sum = reduce(lambda c, s: c + s, ltest_odd_squares)
    print(ltest_odd_squares_sum)

    fh = open("filename", "r")
    arr = fh.readlines()

    arr = fh.readline()
    for line in fh:
        pass

    fh.w
    # ltest_double = []
    # for nl in ltest:
    #     print(nl)
    #     ltest_double.append(nl + nl)
    #
    # lmap_square = list(map(lambda x: 2 * x, ltest))
    # lgen_cube = [x * x * x for x in ltest]
    # lgen_2cube = [x + x for x in lgen_cube]
    #
    # print(lgen_2cube)
    #
    # ltest_cube = []
    # for x in ltest:
    #     ltest_cube.append(x * x * x)
    #
    # ltest_2cube = []
    # for x in ltest_cube:
    #     ltest_2cube.append(x + x)

    #
    # for lg in lgen_cube:
    #     print(lg)
    #
    # print(ltest)
    # print(ltest_double)
    # print(lmap_square)
    # print(lgen_cube)

    # dtest = {0: 'zero', 1: 'one'}
    #
    # print(dtest[0])
    #
    # for k in dtest:
    #     print(k, dtest[k])
    #
    # print(len(dtest), len(lgen_cube))
    #

    # print(__name__)
    # some_program()
