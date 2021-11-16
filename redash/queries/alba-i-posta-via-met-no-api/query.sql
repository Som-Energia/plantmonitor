url: "https://api.met.no/weatherapi/sunrise/2.0/.json"
params:
  lat: "36.7201600"
  lon: "-4.4203400"
  date: "{{ date }}"
  offset: "+00:00"
  days: 10
path: location.time
fields: [sunrise.time, sunset.time]