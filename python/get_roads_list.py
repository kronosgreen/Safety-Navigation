
f = open("roads.txt",'r')
o = open("new.txt", 'a')
for line in f:
    newline = line[7:-1] + ','
    print(newline)
    o.write(newline)
