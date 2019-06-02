import json

with open("temp.txt") as file:
    lines = file.readlines()

result = []
for l in lines:
    r = {}
    r['name'] = str(l).strip().replace('\\n','')
    result.append(r)

with open('products_eworldtrade.json', 'w') as outfile:
    json.dump(result, outfile)