import os
import sys
import subprocess
import pathlib
import shutil
from osgeo import gdal
import qgis
from qgis import processing
from qgis.core import (
    QgsApplication,
    QgsProcessingFeedback,
    QgsVectorLayer,
    QgsProject,
    QgsDistanceArea,
    QgsUnitTypes,
)
from qgis.utils import iface
from qgis.analysis import QgsNativeAlgorithms

def main():
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"

    folder = pathlib.Path(r"/home/anton/Documents/GIT/RA/pixel")
    temp = folder / "temp"

    raster_name = r"SVDNB_npp_d20190329.rade9d.tif"  # light raster map
    raster_file = folder / pathlib.Path(raster_name)
    tiles = folder / "tiles" / "COMM_RG_01M_2016_4326.shp"  # map of municipalities

    #out_path = pathlib.Path(r'/home/anton/Documents/GIT/RA/pixel/temp')

    tile_size_x = 1
    tile_size_y = 1

    xsize = 1  # band.XSize
    ysize = 1  # band.YSize
    start_x = 48001
    start_y = 6000
    end_x = 60000
    end_y = 12000
    ###################################################################################

    # Supply path to qgis install location
    QgsApplication.setPrefixPath('/usr', True)
    QgsApplication.setPrefixPath(qgis_path, True)
    # Create a reference to the QgsApplication.  Setting the
    # second argument to False disables the GUI.
    qgs = QgsApplication([], False)
    # Load providers
    qgs.initQgis()

    sys.path.append('/usr/share/qgis/python/plugins')
    from processing.core.Processing import Processing
    Processing.initialize()
    import processing

    ds = gdal.Open(str(raster_file))
    band = ds.GetRasterBand(1)
    print(band.XSize, band.YSize)

    #  iterate over pixels in rectangular zone of input raster
    for i in range(start_x, start_x + xsize, tile_size_x):
        for j in range(start_y, start_y + ysize, tile_size_y):
            os.makedirs(temp, exist_ok=True)
            out_name = temp / ("tile_" + str(i) + "_" + str(j) + ".tif")

            # TODO: keep all intermediate files in memory (but prevent overfloating as before when tried),
            #  so that no I/O and directory removal needed

            # get one pixel from raster by its i and j pixel coordinates
            com_string = "gdal_translate -of GTIFF -srcwin " + str(i) + ", " + str(j) + ", " + str(
                tile_size_x) + ", " + str(tile_size_y) + " " + str(raster_file) + " " + str(out_name)
            os.system(com_string)

            # polygonize pixel
            polygon_filename = "tile_" + str(i) + "_" + str(j)
            subprocess.run(["gdal_polygonize.py", str(out_name), str(temp), polygon_filename])

            pixel_polygon = QgsVectorLayer(str(temp / (polygon_filename + ".shp")), "Polygon layer", "ogr")
            if not pixel_polygon.isValid():
                print("Layer failed to load!")
            else:
                QgsProject.instance().addMapLayer(pixel_polygon)
            """
            for k in pixel_polygon.getFeatures():
                pixel_polygon = k
                break
            print(pixel_polygon)
            """
            # intersect pixel with tiles
            params = {
                "INPUT": str(tiles),
                "INTERSECT": str(temp / (polygon_filename + ".shp")),
                'OUTPUT': str(temp / "tiles_intersect.shp"),
                "PREDICATE": [0]
            }
            feedback = QgsProcessingFeedback()
            processing.run('qgis:extractbylocation', params, feedback=feedback)
            print("Extracted intersecting areas")
            print(pixel_polygon)

            params = {
                "INPUT": str(temp / ("tiles_intersect" + ".shp")),
                "OVERLAY": pixel_polygon,
                "OUTPUT": str(temp / "final_intersect.shp")  #'memory:buffer'
            }
            processing.run("qgis:intersection", params, feedback=feedback)
            print("Intersected with pixel")

            layer = QgsVectorLayer(str(temp / ("final_intersect" + ".shp")), "Final layer", "ogr")
            print(layer.featureCount())
            field_name = "COMM_ID"
            print(layer.fields().indexFromName(field_name), layer.name())
            fields = layer.fields()
            field_names = [field.name() for field in fields]
            print(field_names)
            feature_ids = [feature["COMM_ID"] for feature in layer.getFeatures()]
            print(feature_ids)

            d = QgsDistanceArea()
            d.setEllipsoid('WGS84')
            for feature in layer.getFeatures():
                print(d.convertAreaMeasurement(d.measureArea(feature.geometry()), QgsUnitTypes.AreaSquareKilometers))
            shutil.rmtree(str(temp), ignore_errors=True)

            # Finally, exitQgis() is called to remove the
            # provider and layer registries from memory
            QgsProject.instance().removeAllMapLayers()
            qgs.exit()


if __name__ == '__main__':
    main()