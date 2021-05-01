import os
import platform
import sys

# QGIS imports
#from osgeo import gdal
#import qgis

from qgis.core import (
    QgsApplication,
    #    QgsProcessingFeedback,
    QgsVectorLayer,
    QgsProject,
    QgsDistanceArea,
    QgsUnitTypes,
    # QgsMapLayerRegistry
    QgsFeature,
    QgsPointXY,
    QgsGeometry,
    QgsVectorFileWriter,
    QgsFeatureRequest,
    QgsCoordinateTransformContext,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
)
# from qgis.utils import iface
# from qgis.analysis import QgsNativeAlgorithms
#
# from qgis import processing
# import processing

def load_layer(path_to_tiles_layer, name="tiles",):
    tiles_layer = QgsVectorLayer(path_to_tiles_layer, name, "ogr")
    if not tiles_layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(tiles_layer)
    return tiles_layer


def create_layer_from_geom(geom, layer_type, geom_type):
    """
    """
    feat = QgsFeature()
    feat.setGeometry(geom)
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    return layer


def layer_from_filtered_tiles(tiles_layer_name, expr=None, crs_name="epsg:6933",
                              save_flag=False, save_name=None, get_extent=False,):
    tiles_layer = load_layer(tiles_layer_name, name="tiles",)
    # select tiles by NUTS/COMM_ID
    selected_tiles = tiles_layer.getFeatures(QgsFeatureRequest().setFilterExpression(expr))
    # save selected tiles to the new layer
    geom_type = "multipolygon"
    layer_type = geom_type + '?crs=' + crs_name
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    for feat in selected_tiles:
        prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    # crs = tiles_layer.crs()
    # logging.info(f"Used CRS: {crs.description()}")
    # crs_name = crs.authid()
    save_path = None
    if save_name is not None and save_flag:
        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "ESRI Shapefile"
        error = QgsVectorFileWriter.writeAsVectorFormatV2(layer, os.path.join(os.getcwd(), save_name),
                                                          QgsCoordinateTransformContext(), options, )
        save_path = os.path.join(os.getcwd(), save_name)
        print(error)
    extent = None
    if get_extent:
        ext = layer.extent()
        extent = (ext.xMinimum(), ext.xMaximum(),
                  ext.yMinimum(), ext.yMaximum())
    return layer, extent, save_path #{"layer": layer, "extent": extent, "save_path": save_path}



def create_pixel_area(i, j, geom_type="polygon", tile_size_x=1, tile_size_y=1,
                      crs_source_name="epsg:4326", transform=False, crs_dest_name="epsg:6933",
                      get_area_only=False, metric=None, save_layer=False, save_name="layer.sh",
                      step=0.004166666700000000098, x0=-180.0020833333499866, y0=75.0020833333500008):
    # parameters of the raster layer
    x2 = 180.0020862133499975
    y2 =  -65.0020844533500082
    x_size = 86401
    y_size = 33601

    if geom_type == "point":
        # center of square area
        p = QgsPointXY(x0 + (i + tile_size_x / 2.0) * step, y0 - (j + tile_size_y / 2.0) * step)
        if transform:
            source_crs = QgsCoordinateReferenceSystem(crs_source_name)
            dest_crs = QgsCoordinateReferenceSystem(crs_dest_name)
            tr = QgsCoordinateTransform(source_crs, dest_crs, QgsProject.instance())
            p = tr.transform(p)
        geom = QgsGeometry.fromPointXY(p)
    elif geom_type == "square":
        if transform:
            raise NotImplementedError(f"No crs transform for polygon yet")
        else:
            p1 = QgsPointXY(x0 + i * step, y0 - j * step)
            p2 = QgsPointXY(x0 + (i + tile_size_x) * step, y0 - j * step)
            p3 = QgsPointXY(x0 + (i + tile_size_x) * step, y0 - (j + tile_size_y) * step)
            p4 = QgsPointXY(x0 + i * step, y0 - (j + tile_size_y) * step)
            points = [p1, p2, p3, p4]
            geom = QgsGeometry.fromPolygonXY([points])
            if get_area_only:
                feat = QgsFeature()
                feat.setGeometry(geom)
                if metric is None:
                    metric = QgsDistanceArea()
                    metric.setEllipsoid('WGS84')
                return metric.convertAreaMeasurement(metric.measureArea(feat.geometry()), QgsUnitTypes.AreaSquareKilometers)
    else:
        raise NotImplementedError(f"Not known type {type}")
    if transform:
        layer_type = geom_type+'?crs=' + crs_dest_name
    else:
        layer_type = geom_type + '?crs=' + crs_source_name
    layer = create_layer_from_geom(geom, layer_type, geom_type)
    if save_layer:
        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "ESRI Shapefile"
        error = QgsVectorFileWriter.writeAsVectorFormatV2(layer, os.path.join(os.getcwd(), save_name),
                                                          QgsCoordinateTransformContext(), options, )
        if error[0]:
            print(error[0])
    return layer


def expression_from_nuts_comm(nuts_yes, nuts_no, comm_yes, comm_no):
    # construct QGIS SQL-like expression to choose tiles
    att_name = "NUTS_CODE"
    expr = " AND ".join(map(lambda x: "\"" + att_name + "\" LIKE '%" + x + "%'", nuts_yes))
    if expr and nuts_no:
        expr += " AND "
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" NOT LIKE '%" + x + "%'", nuts_no))
    att_name = "COMM_ID"
    if expr and (comm_yes or comm_no):
        expr += " OR "
    expr += " AND ".join(map(lambda x: "\"" + att_name + "\" = '" + x + "'", comm_yes))
    if expr and comm_no:
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" <> '" + x + "'", comm_no))
    return expr


def delete_layers():
    if 1:
        for l in QgsProject.instance().mapLayers().values():
            QgsProject.instance().removeMapLayer(l.id())
        for l in QgsProject.instance().mapLayers().values():
            print(l.name())
    return 0


class QGISContextManager:
    def __init__(self):
        if platform.system() == "Linux":
            self.qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"
            QgsApplication.setPrefixPath('/usr', True)
            sys.path.append('/usr/share/qgis/python/plugins')
            path_to_gdal = None
        elif platform.system() == "Windows":
            self.qgis_path = r"C:\\ProgramData\\Anaconda3\\envs\\qgis\\Library\\python\\qgis\\"
            path_to_gdal = r"C:\ProgramData\Anaconda3\envs\qgis\Scripts"
        else:
            self.qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"
            assert "Not Windows or Linux, not guaranteed to work"
        # Supply path to qgis install location
        QgsApplication.setPrefixPath(self.qgis_path, True)  # not needed?

        self.qgs = None

    def __enter__(self):
        # Create a reference to the QgsApplication.  Setting the second argument to False disables the GUI.
        self.qgs = QgsApplication([], False)
        # Load providers
        self.qgs.initQgis()

        from processing.core.Processing import Processing
        Processing.initialize()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # Finally, exitQgis() is called to remove the provider and layer registries from memory
        self.qgs.exit()