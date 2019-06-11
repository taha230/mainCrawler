import json

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup


def html_to_json(content, indent=None):
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
            for i in range(headers):
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