from pathlib import Path
import ast

def spanishDateToISO(spanishDate, spanishHourMinutes):
    return "{}-{:02}-{:02}T{:02}:{:02}:00+02:00".format(
        *([int(x) for x in 
            spanishDate.split('/')[::-1]+
            spanishHourMinutes.split(":")
        ])
    )

def readTimedDataTsv(filename, columns):
    data = Path(filename).read_text(encoding='utf8')
    data= [
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
            spanishDateToISO(line[0],line[1]),
        ] + [
            ast.literal_eval(line[index].replace(',','.'))
            for index in columnIndex
        ]
        for line in data[2:]
    ]



