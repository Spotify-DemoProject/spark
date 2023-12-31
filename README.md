# spark
Spotify API의 response 데이터(.json)을 가공하여 .parquet 데이터를 생성하는 상시 동작 어플리케이션입니다.<br>
Kafka 채널을 구독하고 있으며, 해당 채널에 발행된 메세지를 기반으로 데이터 가공 작업을 수행합니다.<br><br>

# Structure
<img width="717" alt="스크린샷 2024-01-01 오전 1 21 27" src="https://github.com/Spotify-DemoProject/spark/assets/130134750/c0d03957-abdf-4833-bcb5-26a22c5c73dc"><br><br>

# Environments
- Ubuntu v22.04 LTS
- Python v3.12.0
- Spark v3.5.0<br><br>

# Results
<img width="721" alt="스크린샷 2023-12-30 오후 9 57 48" src="https://github.com/Spotify-DemoProject/spark/assets/130134750/cc222dff-0e84-4394-9d4f-ecd921717ec5"><br>
생성된 .parquet 데이터는 AWS S3 버킷 내부에 적재됩니다.
