#!/usr/bin/env python3

for i in range(1000):
    if i % 2 == 0:
        phone = "412-888-8888" 
    else:
        phone = "412-999-9999" 
    text = "W tbl1 ("+str(i)+", name"+str(i)+", "+phone+")"
    print(text)
