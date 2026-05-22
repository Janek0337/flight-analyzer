# flight-analyzer

Projekt sprawdza, jak pogoda wplywa na opoznienia w ruchu lotniczym.

Model nie przewiduje pojedynczego lotu. Dane EUROCONTROL sa zagregowane po
podmiotach/obszarach `ENTITY_NAME`, wiec model przewiduje `delay_per_flight`,
czyli srednie opoznienie dla danego dnia i obszaru.

## Jak Powstaja Dane Dla Modelu

Pipeline bierze dwa zrodla danych:

```text
EUROCONTROL -> opoznienia i liczba lotow
Open-Meteo  -> historyczna pogoda dla lotnisk
```

Najpierw Spark liczy dzienne opoznienia z danych EUROCONTROL. Powstaja m.in.:

```text
date
ENTITY_NAME
ENTITY_TYPE
total_flights
delay_per_flight
```

Osobno Spark liczy dzienna pogode dla kazdego lotniska z `spark/airports.csv`:

```text
icao
date
avg_temperature
total_rain
total_snowfall
avg_wind_speed
max_wind_gusts
```

Potem `build_features.py` laczy te dane. `ENTITY_NAME` jest mapowane na
reprezentatywne lotnisko, np.:

```text
DFS            -> EDDF Frankfurt
DSNA           -> LFPG Paris Charles de Gaulle
ENAIRE         -> LEMD Madrid Barajas
Austro Control -> LOWW Vienna
```

Dzieki temu rekord opoznien dostaje lokalna pogode z tego samego dnia:

```text
date + ENTITY_NAME -> date + weather_station_icao
```

Finalne dane dla modelu sa zapisane tutaj:

```text
hdfs://nn1:9000/bigdata/flight_delay/processed/features/daily_features_parquet
```

## Co Bierze Model

Model uzywa m.in.:

```text
ENTITY_NAME
ENTITY_TYPE
weather_station_icao
total_flights
weather_match_level
feature_temperature
feature_humidity
feature_precipitation
feature_rain
feature_snowfall
feature_cloud_cover
feature_wind_speed
feature_wind_gusts
day_of_week
month_of_year
day_of_week_sin
day_of_week_cos
day_of_year_sin
day_of_year_cos
previous_delay_per_flight
rolling_7d_delay_per_flight
rolling_14d_delay_per_flight
previous_total_flights
is_precipitation_day
is_rainy_day
is_snowy_day
is_windy_day
temperature_below_zero
rain_x_wind_speed
snow_x_below_zero
```

Celem predykcji jest:

```text
delay_per_flight
```

Czyli model uczy sie z historii:

```text
dany obszar + dana pogoda + liczba lotow + dzien/ miesiac
=> srednie opoznienie
```

Po pierwszej wersji modelu dodalismy kilka ulepszen cech:

```text
OneHotEncoder dla kategorii
cechy sezonowe sin/cos z dnia tygodnia i dnia roku
opoznienie z poprzedniego dnia dla danego ENTITY_NAME
srednie opoznienie z ostatnich 7 i 14 dni
liczba lotow z poprzedniego dnia
flagi pogodowe: deszcz, snieg, opady, silny wiatr, temperatura ponizej zera
interakcje pogodowe: deszcz * wiatr oraz snieg * temperatura ponizej zera
```

Te cechy pomagaja regresji liniowej, bo model nie dostaje juz tylko surowych
wartosci pogody i nazw obszarow. Dostaje tez informacje o sezonowosci,
historii opoznien i prostych sytuacjach pogodowych, ktore moga zwiekszac
ryzyko opoznien.

## Jakiego Modelu Uzywamy

Model jest zbudowany w Spark ML jako `Pipeline`. W srodku sa proste kroki:

```text
StringIndexer    -> przygotowuje tekstowe kategorie do kodowania
OneHotEncoder    -> koduje kategorie bez sztucznego porzadku liczbowego
Imputer          -> uzupelnia braki w kolumnach liczbowych
VectorAssembler  -> sklada wszystkie cechy w jedna kolumne features
LinearRegression -> trenuje model regresji liniowej
```

`OneHotEncoder` jest wazny, bo nazwy takie jak `DFS`, `DSNA` albo `ENAIRE` nie
maja naturalnej kolejnosci. W samej regresji liniowej nie chcemy, zeby model
traktowal jedna nazwe jako "wieksza" od drugiej tylko dlatego, ze dostala
wiekszy indeks liczbowy.

Glowny model predykcyjny to:

```text
pyspark.ml.regression.LinearRegression
```

Czyli jest to regresja liniowa, ktora przewiduje wartosc liczbowa:

```text
delay_per_flight
```

Do sprawdzania wyniku uzywamy:

```text
RMSE - sredni blad predykcji
MAE  - sredni blad bezwzgledny predykcji
R2   - jak dobrze model tlumaczy zmiennosc danych
```

Po treningu metryki sa zapisywane jako JSON, razem z podstawowymi informacjami
o uruchomieniu: liczba rekordow treningowych/testowych, uzyte cechy, sciezka
wejsciowa, sciezka modelu oraz wartosci `RMSE`, `MAE` i `R2`.

Zapisujemy tez metryki baseline'u. Baseline to prosty punkt odniesienia, ktory
zawsze przewiduje srednia wartosc opoznienia ze zbioru treningowego. Dzieki
temu widzimy, czy model jest faktycznie lepszy od bardzo prostego przewidywania.

Po aktualizacji cech uzyskane metryki na zbiorze testowym byly:

```text
RMSE: 0.1716
MAE:  0.0954
R2:   0.5671
```

Dla baseline'u:

```text
Baseline RMSE: 0.3210
Baseline MAE:  0.2717
Baseline R2:  -0.5149
```

Wynik `R2 = 0.5671` oznacza, ze model wyjasnia okolo 56.7% zmiennosci srednich
opoznien w zbiorze testowym. `MAE = 0.0954` oznacza sredni blad bezwzgledny
okolo 0.095 minuty, czyli okolo 5.7 sekundy na lot.

Domyslna sciezka metryk:

```text
hdfs://nn1:9000/bigdata/flight_delay/models/local_weather_delay_regression_metrics
```

## Skad Sa Nazwy ENTITY_NAME

`ENTITY_NAME` nie wymyslamy sami. Ta kolumna pochodzi z plikow EUROCONTROL
`ERT DLY ANSP`, czyli danych o opoznieniach w europejskim ruchu lotniczym.

W tych danych `ENTITY_NAME` oznacza podmiot albo obszar odpowiedzialny za ruch
lotniczy, najczesciej instytucje kontroli ruchu lotniczego dla danego kraju lub
regionu.

Przyklady:

```text
DFS            -> Niemcy
DSNA           -> Francja
ENAIRE         -> Hiszpania
Austro Control -> Austria
AirNav Ireland -> Irlandia
Fintraffic ANS -> Finlandia
```

W `process_flights.py` Spark grupuje dane po:

```text
date + ENTITY_NAME + ENTITY_TYPE
```

Dzieki temu wiemy, jakie bylo srednie opoznienie dla danego obszaru w danym dniu.

Potem w `build_features.py` mapujemy `ENTITY_NAME` na reprezentatywne lotnisko
pogodowe, np.:

```text
DFS    -> EDDF Frankfurt
DSNA   -> LFPG Paris Charles de Gaulle
ENAIRE -> LEMD Madrid Barajas
```

To mapowanie sluzy tylko do dobrania lokalnej pogody. Sama nazwa `ENTITY_NAME`
nadal zostaje w danych i model moze sie uczyc, ze rozne obszary maja rozne
historyczne wzorce opoznien.

## Uruchomienie Modelu

```bash
spark-submit model/model.py \
  --input hdfs://nn1:9000/bigdata/flight_delay/processed/features/daily_features_parquet \
  --output hdfs://nn1:9000/bigdata/flight_delay/models/local_weather_delay_regression \
  --metrics-output hdfs://nn1:9000/bigdata/flight_delay/models/local_weather_delay_regression_metrics
```

Caly pipeline:

```bash
./run_pipeline.sh
```
