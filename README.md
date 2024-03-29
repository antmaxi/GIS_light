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

`python dwld.py --data_type nightly --sub_type rade9d`

Data [description](https://eogdata.mines.edu/products/vnl/)

Remark: in rade9d files from the dates (20190331, 20190531, 20190731), and in cloud_cover - 20191031, 
all are missing on the site of EOG.

### Pixel processing
`qgis_utils.py` — useful functions to work with QGIS layers

`control_raster_process.py` —  to run in batches `raster_label.py` or `measure_dist.py` - labeling of raster pixels according 
to the municipalities they intersect and getting these areas

Needs: installed and configured QGIS\
It could be done by adding the path to qgis in the 
environment (also called "qgis" here) e.g. for Linux - adding to 
`~.bashrc` this line:\
`export PATH="home/[user]/anaconda3/envs/qgis/bin:$PATH"`,
in Windows this path should be added to the environment 
variable `PATH`
(see also `QGISManagerContext` for used setup)

Usage:
For example to run (for `--id_y` from `0` to `3` 
with totally 4 parts):

`python control_raster_process --code ES -m 4 --id_x 0`

In order to label land-use just take `--code LAND`
(need to change path to the file in code now):

#### Optimizations:
1) hierarchical labeling: starting from big areas, e.g. 40x40 pixels, if the whole area is inside one tile, 
all its pixels are labeled correspondingly
2) creation of pixels using QGIS instead of cutting them from the raster file
3) splitting of the initial raster area into several ones and possibility of parallel launch

#### Technical info
1) Tiles are extracted from the general Europe municipalities map by the country name in COMM_ID
   (as some don't have NUTS, but COMM_ID all have).
   The file with all municipalities' tiles is `COMM_RG_01M_2016_4326.shp` 
   on the [google-disk](https://drive.google.com/drive/folders/1bJRAxose2mekKBZHRilsgRgMzOK5i6yl?usp=sharing)
2) Tiles per-country are extracted automatically
3) The extents of tiles are taken from the properties of corresp. layers (the same link as before) automatically and then the rectangulars 
   from pixels are circumscribed around them with some gap
   
### Distances between pixel and area

Using QGIS plugin [NNJoin](http://arken.nmbu.no/~havatv/gis/qgisplugins/NNJoin/), 
which finds the Cartesian distance from pixel's center point to the closest tile from the set of tiles
(e.g. municipalities under lockdown or neighboring border tiles)

All Scotland's municipalities NUTS are set as UKM.

Algorithm:
1) It first finds the bordering tiles of the selected by the code country.
2) Also processes file with data on lockdowns to get different lockdown configuration
and the corresponding dates.
3) Then going through the prepared list of labeled pixels from the needed
country, checks whether they are in/out in relation to lockdown.
   4) For the corresp. to the needed type of calc pixels finds 
      the distance to the bordering tiles, and info about them 
      (NUTS, municipality ID).
To run e.g.:

`python measure_dist.py --alg_type get_dist_lockdown 
--alg_subtype to_lockdown --code FR`

`alg_subtype == to_lockdown` means finding distances from non-lockdown
areas inside the country to lockdown border tiles
`alg_subtype == from_lockdown` means the opposite - from lockdown
areas inside the country to non-lockdown border tiles

Features:
1) pixels touching both lockdown and non-lockdown would appear in
both "to" and "from" calculations 
   (but obviously with small distances)
   
Possible TODOs:
1) for optimization maybe take not all the pixels in the area, but
   1) only which were already labeled
   2) which are not too far from the extents of the lockdown area 
      (e.g. circumscribe around it and check only inside it). 
      Now it can process around 5 pixels/s, which is not much.
      Esp. taking into account that we will have to do it several times for different
      lockdown areas
      
### Files` auxiliary processing
1) `merge_csv.py --code ES` — to merge divided (with `-n` and `-m` arguments) 
   results of labeling into one `.csv` file

For example to run:

`python merge_csv.py --code ES` or `python merge_csv.py --pattern results/pixel_intersections_Fr*.csv`

2) `merge_dist_lockdown_files.py --code ES` —  to merge files 'to' and 'from' lockdown for all the dates and for the input code of country

3) `tiff_to_csv.py --mode cloud_cover` (or with other modes) 
   1) downloads 
   full .tiff files from the shared folder 
   (checking which ones are already processed), 
   2) crops Western Europe (pixels in pixel coords x: (40272, 49921) (dx=9650)
    y: (2808, 10441) (dy=7634) included; 
   in degrees it's about (-12.2, 31.5, 28, 63.3))
      3) converts to .parquet and .csv (using ID inside the cropped area
         starting from 0 and ending with 7,366,899 and rounding float 
         values to one decimal to compress the data, as it's step for "rade9d" 
         is 0.1)
   4) uploads results to the shared folder. 