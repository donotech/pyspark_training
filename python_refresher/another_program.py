#from python_basics import some_program
import python_basics

from python_basics import Participants

print("another program")
#python_basics.some_program()

p1 = Participants('a', 18, 9000)
p2 = Participants('b', 17, 9000)
p1.city = 'BLR'
p2.city = 'CH'
print(p1.print_values())
print(p2.print_values())
