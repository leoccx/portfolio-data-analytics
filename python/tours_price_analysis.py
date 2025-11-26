import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use("ggplot")

# 1. загрузка данных

df = pd.read_csv("tours_prices.csv")
df.head()

# 2. форматирование и чистка данных

df.columns = df.columns.str.lower().str.strip()

df["price"] = df["price"].str.replace(" ", "").astype(float)

df = df.drop_duplicates()
df = df[df["price"] > 0]

# 3. анализ динамики цен

plt.figure(figsize=(12,5))
sns.lineplot(data=df, x="date", y="price", hue="country")
plt.title("Динамика цен на туры по странам")
plt.show()

# 4. средняя цена по странам

df.groupby("country")["price"].mean().sort_values().plot(kind="bar", figsize=(10,5))
plt.title("Средняя цена по странам")
plt.show()

# 5. зависимость цены от рейтинга/отеля/времени вылета

sns.scatterplot(data=df, x="rating", y="price", alpha=0.5)
plt.title("Цена vs рейтинг отеля")
plt.show()

# 6. результаты

print("""
1. Самые доступные направления: …
2. Самые дорогие направления: …
3. Рейтинг отеля положительно коррелирует с ценой (R≈...).
4. Рекомендации турагентству: …
""")


