import requests

url = "https://api.benzinga.com/api/v2.1/calendar/ratings/analysts"

querystring = {"token":"9f4b718a05a44b2ba4c48a5fff692647"}

response = requests.request("GET", url, params=querystring)

print(response.text)
