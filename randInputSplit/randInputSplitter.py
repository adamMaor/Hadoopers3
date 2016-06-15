import random

database = open('movies-simple4.txt', 'r')
test = open('test.txt', 'w')
train = open('train.txt', 'w')

database_lines = database.readlines()
for line in database_lines:
    if (random.random() >= 0.8):
        test.write(line);
    else:
        train.write(line);

test.close
train.close
        
