## Installation
`conda install --file environment.yml`

## Functions
### Raster data downloading
`dwld.py` —  download multiple GeoTIff images from https://eogdata.mines.edu/nighttime_light

Usage: should be configured parameters:
1) `data_type` ("monthly", "nightly") 
and if "nightly", then `sub_type` from ("cloud_cover", "rade9d")
2) the starting date `time_min` (default - 01.01.2018) and ending `time_max` (default - today) in format "YYYYMMDD"

For example to run:

`python dwld.py --data_type nightly --sub_type radeg9d`

Data description https://eogdata.mines.edu/products/vnl/

### Pixel labeling
`control_raster_label.py` —  to run in batches `raster_label.py` - labeling of raster pixels according 
to the municipalities they intersect and getting these areas

Needs: installed and configured QGIS

Usage:
For example to run (for `--id_x_curr` from `0` to `3`):

`python control_raster_label.py --python control_raster_label.py -n 4 
--n_all 88 -m 56 --tilename Spain --country Spain --id_x_curr 0 `

Optimizations:
1) hierarchical labeling: starting from big areas, e.g. 40x40 pixels, if the whole area is inside one tile, 
all its pixels are labeled correspondingly
2) creation of pixels using QGIS instead of cutting them from the raster file
3) splitting of the initial raster area into several ones and possibility of parallel launch

#### Technical info
1) Tiles are extracted from the general Europe municipalities map by the country NUTS 
   (`COMM_RG_01M_2016_4326.shp` 
   from [google-disk](https://drive.google.com/drive/folders/1bJRAxose2mekKBZHRilsgRgMzOK5i6yl?usp=sharing))
2) The extents of tiles are taken from the properties of corresp. layers (the same link as before) and then the rectangulars 
   from pixels are circumscribed around them with some gap
   
### Labels` processing
`merge_csv.py` — to merge divided results of labeling into one `.csv` file

For example to run:

`python merge_csv.py --country Spain` or `python merge_csv.py --pattern results/pixel_intersections_Fr*.csv`

### Distances between pixel and area

Using QGIS plugin [NNJoin](http://arken.nmbu.no/~havatv/gis/qgisplugins/NNJoin/), 
which finds the Cartesian distance from pixel to the closest tile from the set of tiles
(e.g. municipalities under lockdown)

TODO: 

1) select municipalities by NUTS and/or COMM_ID, which should be taken/disregarded
2) for optimization maybe take not all the pixels in the area, but
   1) only which were already labeled
   2) which are not too far from the extents of the lockdown area 
      (e.g. circumscribe around it and check only inside it). 
      Now it can process around 5 pixels/s, which is not much.
      Esp. taking into account that we will have to do it several times for different
      lockdown areas
