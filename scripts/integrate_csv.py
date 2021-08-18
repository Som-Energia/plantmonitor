import sys
import datetime
import scipy
from itertools import groupby

def readTimeseriesCSV(csv_filepath):
   with open(csv_filepath) as csv_file:
     content = csv_file.read()
     timeseries = [
       (
         datetime.datetime.fromisoformat(l[0]).astimezone(datetime.timezone.utc),
         int(l[1])
       ) for l in (r.split(',') for r in content.split('\n') if r)
     ]

   return timeseries

def sliceTS(sensorTS):

  # this groupby splits into three lists every hour, e.g. [10:00], [10:05, ..., 10:55], [11:00]
  sliced = [list(g) for k, g in groupby(sensorTS, lambda r: r[0].minute == 0)]

  # for each hour, join the three lists (start, middle, end) e.g. [10:00, 10:05, ..., 10:55, 11:00]
  return [sliced[i] + sliced[i+1] + sliced[i+2] for i in range(len(sliced)-2) if i%2]

def fillHoles(sensorSlice):
  sensorSliceFilled = []

  t = datetime.datetime.max - datetime.timedelta(hours=24, minutes=7)
  t = t.replace(tzinfo=datetime.timezone.utc)

  for reading in sensorSlice:

    while t + datetime.timedelta(minutes=7) < reading[0]:
      t += datetime.timedelta(minutes=5)
      sensorSliceFilled.append((t,0))
    sensorSliceFilled.append(reading)
    t = reading[0]

  return sensorSliceFilled

def integrate(filename):

  sensorTS = readTimeseriesCSV(filename)

  sensorSlices = sliceTS(sensorTS)

  irradiationTS = []
  for sensorSlice in sensorSlices:
    sensorSlice.reverse()

    sensorSlice = fillHoles(sensorSlice)

    yvalues = [y for x,y in sensorSlice]
    xvalues = [x.timestamp()/3600 for x,y in sensorSlice]

    irradiation = scipy.trapz(yvalues, xvalues)

    hour = sensorSlice[0][0].replace(minute=0, second=0, microsecond=0)

    irradiationTS.append((hour, irradiation))

  return irradiationTS

def main():

  filename = sys.argv[1]
  outfilename = sys.argv[2]

  print("Compute integral for {} to {}".format(filename, outfilename))

  result = integrate(filename)

  print(result)

  print("Job's Done, Have A Nice Day")

if __name__ == "__main__":
  main()
