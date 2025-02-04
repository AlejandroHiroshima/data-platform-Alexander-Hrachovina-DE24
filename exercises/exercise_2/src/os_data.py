import pandas as pd
import matplotlib.pyplot as plt

# Läs CSV-filen
df = pd.read_csv("athlete_events.csv")

# Skriv ut de första raderna
print("\nFirst few rows of the dataset:")
print(df.head())

# Skapa en enkel visualisering (exempel: antal atleter per år)
plt.figure(figsize=(10, 6))
df['Year'].value_counts().sort_index().plot(kind='bar')
plt.title('Number of Athletes by Year')
plt.xlabel('Year')
plt.ylabel('Count')

# Spara grafen
plt.savefig('athletes_plot.png')
print("\nPlot has been saved as 'athletes_plot.png'")