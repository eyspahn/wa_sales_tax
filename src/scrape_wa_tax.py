import requests
from bs4 import BeautifulSoup
 
url = 'https://dor.wa.gov/about/statistics-reports/detailed-tax-data-industry-and-tax-classification'
response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

root_url = "https://dor.wa.gov"

hrefs = []

for a in soup.find_all('a', href=True):
    if a['href'].endswith('.xlsx'):
        file_url = root_url + a['href']
        filename = a['href'].split('/')[-1]
        filepath = '../data/WA_tax/'+filename
        file_response = requests.get(url)

        with open(filepath, mode="wb") as file:
            file.write(response.content)
        print(f'Downloaded file {filepath}')

