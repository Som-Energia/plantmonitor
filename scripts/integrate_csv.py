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

  sliced = [list(g) for k, g in groupby(sensorTS, lambda r: r[0].minute == 0)]
  return [sliced[i] + sliced[i+1] + sliced[i+2] for i in range(len(sliced)-2) if i%2]


def integrate(filename):

  sensorTS = readTimeseriesCSV(filename)

  sensorSlices = sliceTS(sensorTS)

  irradiationTS = []

  for sensorSlice in sensorSlices:
    sensorSlice.reverse()
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
