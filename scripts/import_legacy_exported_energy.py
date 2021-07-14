#!/usr/bin/env python3

# Ongoing experiment: taking historical plant production from GDrive
# document from Projects Team.

import datetime
from pathlib import Path
from sheetfetcher import SheetFetcher
from consolemsg import step, warn
from dbutils import csvTable, fetchNs
import psycopg2
from conf import envinfo

psycopgconfig = {
    key: value
    for key, value in envinfo.DB_CONF.items()
    if key != 'provider'
    and value != ''
}

months = (
    "Gener Febrer Març Abril "
    "Maig Juny Juliol Agost "
    "Setembre Octubre Novembre Desembre"
).split()

inecodes = {
    name : code
    for code, name in [
        ('25120','Lleida'),
        ('17148','Riudarenes'),
        ('25228','Torrefarrera'),
        ('08112','Manlleu'),
        ('46193','Picanya'),
        ('25230','Torregrossa'),
        ('47114','Peñafiel'),
        ('41006','Alcolea del Rio'), # tilde mising in drive
        ('41055','Lora del Rio'), # tilde mising in drive
        ('05074','Fontiveros'),
        ('04090','Tahal'),
        ('18152','Pedro Martínez'),
    ]
}

plantCodes = {
    "Zona Esportiva": "Riudarenes_ZE",
    "Semursa": "Riudarenes_SM",
    "Brigada": "Riudarenes_BR",
    "Manlleu Pavello": "Manlleu_Pavello",
    "Manlleu Piscina": "Manlleu_Piscina",
    "Lleida": "Lleida",
    "Picanya": "Picanya",
    "Torrefarrera": "Torrefarrera",
    "Torregrossa": "Torregrossa",
    "Valteina": "Valteina",
    "Alcolea del Rio": "Alcolea",
    "La Matallana": "Matallana",
    "Fontivsolar": "Fontivsolar",
    "La Florida": "Florida",
    "Tahal": "Terborg",
    "Llanillos": "Llanillos",
}


def firstOfNextMonth():
    d = datetime.date.today()
    if d.month==12:
        return str(datetime.date(d.year+1, 1, 1))
    return str(datetime.date(d.year, d.month+1, 1))

def sheetFromDrive(certificate, document, sheet):
    #step('Baixant produccio...')
    fetcher = SheetFetcher(
        documentName=document,
        credentialFilename=certificate,
    )
    info = fetcher.get_fullsheet(sheet)
    info = [[x.strip() for x in row] for row in info]
    return info

def fromCsv(filename):
    return [
        [x.strip() for x in row.split("\t")]
        for row in Path(filename).read_text(encoding='utf8').split('\n')
    ]

def toCsv(filename, data):
    Path(filename).write_text(
        "\n".join(
            "\t".join(
                str(cell) for cell in row
            )
            for row in data
        )
    )

def toIso(year, month):
    return f"{int(year)}-{months.index(month)+1:02d}-01"

def processPlant(result, plant, city):
    #step(f"Plant {plant} - {city}")
    result[plant] = dict(
        name = plant,
        city = city,
        city_code = inecodes[city],
        vals = {}
    )

def processDataItem(result, plant, year, month, value):
    value = value.replace('.','')
    value = value or '0'
    value = int(value)
    date = toIso(year,month)
    #step(f"Data {plant} {date} {year} {month} {value}")
    result[plant][date] = value


def parseblock(info, result):
    plants = [
        (i, plant, city)
        for i, (plant, city) in enumerate(zip(info[0],info[1]))
        if plant.strip()
    ]
    for column, plant, city in plants:
        if plant == 'MWh': break # HACK for the additional info at the end
        processPlant(result, plant, city)
        year = None
        for irow, row in enumerate(info[2:],2):
            key, val = row[column:column+2]
            if not key and not val: continue
            if year is None and key.isnumeric() :
                year = key
                continue
            if key in months:
                month = key
                production_kwh = val
                processDataItem(result, plant, year, month, production_kwh)
                if month == months[-1]:
                    year = None
                continue
            if key:
                warn(f"repeat at row {plant} {year} {irow} {key}")
                break

            #warn("Ignored '{}' -> '{}'",
            #    row[column], row[column+1]
            #)

def headerBlocks(rows):
    """
    Since a second set of plants are on rows below the first set,
    this function detects the rows for the plant headers, and splits
    a block for each set.
    """
    #step("Detecting plant headers")
    headerRows=[]
    for irow, row in enumerate(rows):
        if not row[0]: continue # empty row
        if row[0] in months: continue # month name
        if row[0].isnumeric(): continue # year
        if irow-1 in headerRows: continue # city row, next to plant header row
        headerRows.append(irow) # That's it!
    # once we have the header row indexes, slice rows in blocks
    return [
        rows[i:j]
        for i,j in zip(
            headerRows,
            headerRows[1:]+[None]
        )
    ]

def resultAsTable(result, dates=[]):
    """
    Turns objects to a table
    """
    alldates = set(dates)
    for plant in result.values():
        alldates.update(plant.keys())

    for plant in result.values():
        for date in alldates:
            plant.setdefault(date,0)

    headers = ['name', 'city_code', 'city'] + [
        date
        for date in sorted(alldates)
        if date[0].isnumeric()
    #    and date in dates
    ]
    output = [ headers ]
    for plant in result.values():
        output.append(
            [plant[header] for header in headers]
        )
    return output

def plantProductionFromDrive(
    driveCertificate,
    driveDocument,
    driveSheet,
    cacheFile=None,
    outputFile=None,
    dates=[]
):

    if cacheFile and Path(cacheFile).exists():
        # Avoid download to develop parsing faster
        #step("Taking cached version")
        info = fromCsv(cacheFile)
    else:
        #step("Downloading new version from drive")
        info = sheetFromDrive(
            certificate = driveCertificate,
            document = driveDocument,
            sheet = driveSheet,
        )
        if cacheFile:
            toCsv(cacheFile, info)

    blocks = headerBlocks(info)
    result = {}
    for block in blocks:
        parseblock(block, result)
    print(result)

    exportResultAsPlantmonitorTable(result)

    table = resultAsTable(result, dates)
    if outputFile:
        toCsv(outputFile, table)
    return table

def plantProductionSeries(dates):
    data = plantProductionFromDrive(
        driveCertificate='drive-certificate.json',
        driveDocument='Control de instalaciones_Gestió d\'Actius',
        driveSheet='Històric producció',
        cacheFile='historical-production-sheet.csv',
        outputFile='plantproduction-historical.csv',
        dates=[str(d) for d in dates],
    )

    columntypes = dict(
        name = 'VARCHAR',
        city_code = 'VARCHAR',
        city = 'VARCHAR',
    )

    def fieldname(field):
        if field in columntypes:
            return field
        return "count_{}".format(field.replace('-','_'))

    def valueformat(field, value):
        if field in columntypes:
            return "'{}'".format(value)
        return str(value)

    fields = ',\n'.join(fieldname(field) for field in data[0])

    sumcolumns = ',\n'.join(
        'sum({0}) as {0}'.format(fieldname(field))
        for field in data[0]
        if field not in columntypes
    )

    values = ',\n'.join(
        "({})".format(
            ",".join([
                valueformat(field,value)
                for field,value in zip(data[0],row)
            ])
        )
        for row in data[1:]
    )
    return
    query = f"""
    SELECT
            pais.code AS codi_pais,
            pais.name AS pais,
            comunitat.codi AS codi_ccaa,
            comunitat.name AS comunitat_autonoma,
            provincia.code AS codi_provincia,
            provincia.name AS provincia,
            municipi.ine AS codi_ine,
            municipi.name AS municipi,
            {sumcolumns} -- here go the date count columns
    FROM (VALUES
        {values}
    ) item ({fields})
    LEFT JOIN res_municipi AS municipi
            ON item.city_code=municipi.ine
    LEFT JOIN res_country_state AS provincia
            ON provincia.id = municipi.state
    LEFT JOIN res_comunitat_autonoma AS comunitat
            ON comunitat.id = provincia.comunitat_autonoma
    LEFT JOIN res_country AS pais
            ON pais.id = provincia.country_id
    GROUP BY
            codi_pais,
            codi_ccaa,
            codi_provincia,
            codi_ine,
            pais,
            provincia,
            municipi,
            comunitat.name
    ORDER BY
            pais ASC,
            comunitat_autonoma ASC,
            provincia ASC,
            municipi ASC,
            TRUE ASC
    ;
    """
    import dbconfig as config
    db = psycopg2.connect(**config.psycopg)
    with db.cursor() as cursor :
        cursor.execute(query)
        result = csvTable(cursor)
    #print(result)
    return result


def exportResultAsPlantmonitorTable(result):
    name2id = plantName2Id()
    for plant in result.values():
        plantStarted = False
        for key in sorted(plant.keys()):
            if '-' not in key: continue
            if not plantStarted and plant[key] == 0:
                continue
            if key >= '2021-01-01': continue
            plantStarted=True
            plantid = name2id[plantCodes[plant['name']]]
            print(f"{plantid} {key} {plant[key]}")



def plantName2Id():
    query ="""
        SELECT
        id,
        name
        FROM plant
        ;
    """
    db = psycopg2.connect(**psycopgconfig)
    with db.cursor() as cursor :
        cursor.execute(query)
        result = {
            plant.name : plant.id
            for plant in fetchNs(cursor)
        }
    return result

# TODO: Convertir la lista que estamos imprimiendo en un insert


if __name__ == '__main__':
    #plantProductionSeries(['2010-01-01'])
    plantProductionSeries([])




