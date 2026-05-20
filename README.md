# flight-analyzer

## Model liniowy w Spark

Trening bazowego modelu do przewidywania opóźnienia na podstawie cech batchowych:

```bash
python3 spark/scripts/train_linear_model.py
```

Skrypt czyta dane z `daily_features_parquet`, dodaje proste cechy kalendarzowe i trenuje liniowy model regresji do przewidywania `delay_per_flight`.

Cały pipeline uruchomi też trening automatycznie i wypisze metryki modelu w terminalu:

```bash
./run_pipeline.sh
```