#!/usr/bin/env python3

f = open("test_scripts/test_script.txt", "w+")
text = ""
for i in range(100):
    phone = ""
    if i % 2 == 0:
        phone = "412-888-8888" 
    else:
        phone = "412-999-9999" 
    text += "W tbl1 ("+str(i)+", name"+str(i)+", "+phone+")\n"

    print(text)

for i in range(100):
    text += "R tbl1 "+str(i)+"\n"

    print(text)
f.write(text)
f.close()

