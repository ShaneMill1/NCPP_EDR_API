Using the s3 bucket, the ingest uses a name from the L2 products

The layout of the data seems to differ by L2 product. Therefore, created multiple modules for each type of data layout:

./himawari_to_zarr_2D_Lat_Lon.py AHI-L2-FLDK-RainfallRate
./himawari_to_zarr_L3C_SST.py AHI-L2-FLDK-SST
