# A Small-Scale Temperature Forecasting System using Time Series Models Applied in Ho Chi Minh City

Urban living benefits greatly from weather forecasting since it may lower weather-related losses, safeguard public health and safety and promote both economic growth also quality of life. The main goal of this work is to develop a small-scale temperature forecasting system employing a cutting-edge time series model. In order to do so, data on Ho Chi Minh City's temperature is gathered. The performance of several time series models based on machine learning and deep learning is then evaluated for input data of various lengths. To create a small-scale temperature forecasting system, the best model is chosen. The suggested approach is particularly well suited for a smart agricultural indoor temperature forecasting system, which cannot be accomplished with any large-scale temperature forecasting systems.
Published in: 2022 IEEE International Conference on Communication, Networks and Satellite (COMNETSAT)

Date of Conference: 03-05 November 2022

Date Added to IEEE Xplore: 02 January 2023

ISBN Information:

DOI: 10.1109/COMNETSAT56033.2022.9994437

Publisher: IEEE
Conference Location: Solo, Indonesia

# nasa-power-api

Download meteorological data records from Nasa Power.
You will receive data on:

- Temperature (Temperature at 2 Meters, Wet Bulb Temperature at 2 Meters, etc.)
- Humidity/Precipitation (Specific Humidity at 2 Meters, Relative Humidity at 2 Meters, Precipitation)
- Wind/Pressure (Surface Pressure, Wind Speed at 10 meters, etc.)
  You can find a detailed list of all available parameters at: https://power.larc.nasa.gov/data-access-viewer/

# Usage Instructions

Simply edit the config.yaml by specifying your latitude, longitude, start date, and end date values. # Location
lat: 53.524960
lon: -1.627447 # Format: YYYYMMDD # Earliest is 19810101
start_date: 20100301 # Format: YYYYMMDD
end_date: 20210101
Hit run to save the respective .csv file in the project's data directory.
