from pathlib import Path
import ast

def parseDate(date, time):
    date = "{}-{:02}-{:02}" .format(*[
        int(x) for x in (
        date.split('/')[::-1] # inverse order
        if '/' in date else 
        date.split('-')
        )
    ])
    tz = '02:00' if '+' not in time else time.split('+')[1]
    tz = "{:02}:{:02}".format(
        *([int(x) for x in tz.split(":")]+[0])
    )
    time = "{:02}:{:02}:{:02}".format(
        *([int(x) for x in time.split('+')[0].split(':')]+[0])
    )
    return "{}T{}+{}".format(date,time, tz)

def readTimedDataTsv(filename, columns):
    data = Path(filename).read_text(encoding='utf8')
    data = [
        line.split('\t')
        for line in data.split('\n')
        if line.strip()
    ]
    columnIndex = [
        data[1].index(column)
        for column in columns
    ]
    return [
        [
            parseDate(line[0],line[1]),
        ] + [
            ast.literal_eval(line[index].replace('.','').replace(',','.'))
            for index in columnIndex
        ]
        for line in data[2:]
    ]

