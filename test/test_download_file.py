import unittest
from src.utils import download_file
import os
from pathlib import Path


class TestDownloadFile(unittest.TestCase):

    def test_download(self):
        filepath = Path('data/ersst.v5.185401.nc')
        testurl = 'https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/netcdf/ersst.v5.185401.nc'

        if filepath.exists():
            filepath.unlink()

        path = download_file(testurl)
        
        self.assertEqual(path, str(filepath))
        self.assertTrue(filepath.exists())


if __name__ == '__main__':
    unittest.main()