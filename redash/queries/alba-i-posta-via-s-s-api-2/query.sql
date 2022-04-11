url: "https://api.sunrise-sunset.org/json"
params:
  lat: "36.7201600"
  lng: "-4.4203400"
  date: {{ date }}
fields: [status, results.sunrise, results.sunset]