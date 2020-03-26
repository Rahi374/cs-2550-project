#/usr/bin/env python3

f = open("test_scripts/deletes_script.txt", "w+")
text = ""
for i in range(3):
    phone = ""
    if i % 2 == 0:
        phone = "412-888-8888" 
    else:
        phone = "412-999-9999" 
    text += "W tbl1 ("+str(i)+", name"+str(i)+", "+phone+")\n"


for i in range(1000000):
    phone = ""
    if i % 2 == 0:
        phone = "412-888-8888" 
    else:
        phone = "412-999-9999" 
    text += "E tbl1 1\n"

f.write(text)
f.close()

