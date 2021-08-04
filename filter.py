from sys import argv
from random import randint

filename, inputfilename, outputfilename, size = argv

lines = [line for line in open(inputfilename, 'r')]
outfile = open(outputfilename, 'w')

dimensions = [0, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
dos = ['normal', 'back', 'land', 'neptune', 'pod', 'smurf', 'teardrop']

k = 0
count = 0
attack_count = 0
while count < int(size):
    if k % 1000 == 0:
        print(k)
    k += 1
    index = randint(0,len(lines)-1)
    line = lines.pop(index)
    attributes = line.split(',')
    attributes.pop() # pop off the difficulty
    label = attributes.pop() # get the label
    if label in dos:
        toWrite = str(count) + ','
        for dim in dimensions:
            toWrite += attributes[dim] + ','
        toWrite += label
        outfile.write(toWrite + '\n')
        if label != 'normal':
            attack_count += 1
        count+=1
print('Number of attacks: ' + str(attack_count))
outfile.close()