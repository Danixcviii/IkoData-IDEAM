import xarray as xr
import pandas as pd

# Open the NetCDF file as an xarray Dataset
ds = xr.open_dataset('ersst.v5.185401.nc')

# Convert the xarray Dataset to a pandas DataFrame
df = ds.to_dataframe()

# Optional: Reset the index to make dimensions regular columns
df = df.reset_index()

# View the first 5 rows of the DataFrame
print(df.head())

# Optional: Save the DataFrame to a CSV file
df.to_csv('output_data.csv', index=False, header=True)