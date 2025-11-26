import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = "https://example.com/tours?page="   # пример

tours = []

for page in range(1, 6):
    url = BASE_URL + str(page)
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    items = soup.find_all("div", class_="tour-card")

    for item in items:
        name = item.find("h2").text.strip()
        country = item.find("span", class_="country").text.strip()
        price = item.find("span", class_="price").text.strip()
        rating = item.find("span", class_="rating").text.strip()

        tours.append({
            "name": name,
            "country": country,
            "price": price,
            "rating": rating
        })

    time.sleep(1)

df = pd.DataFrame(tours)
df.to_csv("parsed_tours.csv", index=False)

print("Готово! Сохранено:", len(df), "записей.")
