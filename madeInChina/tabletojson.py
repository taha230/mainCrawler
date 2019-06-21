import json

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup


def table_to_json(content, indent=None):
    '''
    function_name: table_to_json
    input: string
    output: json
    description: get html of table and return json of contents in table
    '''

    soup = BeautifulSoup(content, "lxml")
    rows = soup.find_all("tr")

    headers = []
    thead = soup.find_all("th")
    if thead:
        for i in range(0,len(thead)):
            headers.append(thead[i].text.strip())


    if thead:
        data = []
        for row in rows:
            temp = {}
            cells = row.find_all("td")
            if (len(cells) > 0):
                for i in range(0,len(headers)):
                    temp[str(headers[i]).replace(' ','_')] = str(cells[i]).text.strip()
                data.append(temp)
        return data
    else:
        data = {}
        for row in rows:
            cells = row.find_all("td")
            if (len(cells) % 2 == 0):
                for i in range(0, len(cells), 2):
                    data[str(cells[i].text.strip()).replace(' ', '_')] = cells[i + 1].text.strip()
        return data

def table_to_json_complex(tables, indent=None):
    '''
    function_name: table_to_json_complex
    input: list of table tag
    output: json
    description: get list of html table tags and return json of contents in tables
    '''

    soup1 = BeautifulSoup(str(tables[0]), "lxml")
    soup2 = BeautifulSoup(str(tables[1]), "lxml")

    headers = []
    thead = soup1.find_all("th")
    if thead:
        for i in range(0,len(thead)):
            if("Verified" not in str(thead[i].text.strip())):
                headers.append(thead[i].text.strip())

    rows = soup2.find_all("tr")

    data = []
    for row in rows:
        temp = {}
        cells = row.find_all("td")
        for i in range(0,len(headers)):
           temp[str(headers[i]).replace(' ','_')] = cells[i].text.strip()
        data.append(temp)


    return data