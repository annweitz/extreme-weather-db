import cdsapi

dataset = "reanalysis-era5-single-levels"
request = {
            "product_type": ["reanalysis"],
            "date": "2018-01-01",
            "time": [
                "00:00", "01:00", "02:00",
                "03:00", "04:00", "05:00",
                "06:00", "07:00", "08:00",
                "09:00", "10:00", "11:00",
                "12:00", "13:00", "14:00",
                "15:00", "16:00", "17:00",
                "18:00", "19:00", "20:00",
                "21:00", "22:00", "23:00"
            ],
            "grid": [0.25,0.25],
            "data_format": "netcdf",
            "download_format": "unarchived",
            "variable": ["instantaneous_10m_wind_gust"]
        }

client = cdsapi.Client()
client.retrieve(dataset, request).download()