import csv

new_file = []
key_dict = {}
with open("da.edgelist") as csvedgelist:
    reader = csv.reader(csvedgelist,delimiter = " ")
    for row in reader:
        #row = row.split(" ")
        if row[0] in key_dict:
            row[0] = str(key_dict[row[0]])
        else:
            key_dict[row[0]] = len(key_dict)
            row[0] = str(key_dict[row[0]])
        if row[1] in key_dict:
            row[1] = str(key_dict[row[1]])
        else:
            key_dict[row[1]] = len(key_dict)
            row[1] = str(key_dict[row[1]])
                
        row[2] = float(row[3][:-1])
        del row[3]
        #row = " ".join(row)
        new_file.append(row)

with open("modded.edgelist", "w+") as modelist:
    writer = csv.writer(modelist, delimiter=" ")
    for block_row in new_file:
        writer.writerow(block_row)
