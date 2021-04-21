Installation: 'conda install --file environment.yml'

`dwln.py` —  download multiple GeoTIff images from https://eogdata.mines.edu/nighttime_light
Usage: should be configured parameters `data_type` ("monthly", "nightly") 
and if "nightly", then `sub_type` from ("cloud_cover", "rade9d")
Also the starting date `time_min` and ending `time_max` (default - today) in format "YYYYMMD"

Data description https://eogdata.mines.edu/products/vnl/

`control_raster_label.py` —  to run in batches `raster_label.py` - labeling of raster pixels according 
to the municipalities they intersect and getting these areas

Needs: installed and configured QGIS

Usage:

Optimizations:
1) hierarchical labeling: starting from big areas, e.g. 40x40 pixels, if the whole area is inside one tile, 
all its pixels are labeled correspondingly
2) creation of pixels using QGIS instead of cutting them from the raster file
3) splitting of the initial raster area into several ones and possibility of parallel launch
   
`merge_csv.py` — to merge divided results of labeling into one `.csv` file