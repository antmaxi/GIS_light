import csv
import gc
import glob
import math
import os
import platform
import shutil
import sys
import time

# QGIS imports
# from osgeo import gdal
import qgis

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
from qgis import processing
import processing


class QGISContextManager:
    """
        Context manager to work easier with QGIS
        Usually makes the program exit when used the second time, # TODO why
    """

    def __init__(self):
        os.environ["QT_QPA_PLATFORM"] = "offscreen"  # to handle QT error on some systems
        if platform.system() == "Linux":
            self.qgis_path = os.environ['QGIS']  # "/home/anton/anaconda3/envs/arcgis/bin/qgis"
            # QgsApplication.setPrefixPath('/usr', True)
            # sys.path.append('/usr/share/qgis/python/plugins')
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
        self.metric = QgsDistanceArea()
        self.metric.setEllipsoid('WGS84')

    def __enter__(self):
        # Create a reference to the QgsApplication.  Setting the second argument to False disables the GUI.
        self.qgs = QgsApplication([], False)
        # Load providers
        self.qgs.initQgis()

        from processing.core.Processing import Processing
        Processing.initialize()
        # print("Please ignore 'Application path not initialized' line")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # Finally, exitQgis() is called to remove the provider and layer registries from memory
        # QgsProject.instance().removeAllMapLayers()
        # delete_layers()
        self.qgs.exit()


def load_layer(path_to_tiles_layer, name=None, ):
    """
        Load vector layer to QGIS using its path
    """
    tiles_layer = QgsVectorLayer(path_to_tiles_layer, name, "ogr")
    if not tiles_layer.isValid():
        print(f"Layer {path_to_tiles_layer} failed to load!")
    else:
        QgsProject.instance().addMapLayer(tiles_layer)
    return tiles_layer


def get_save_path(save_path=None, save_name=None):
    """
        Auxiliary for saving
    """
    if save_path is None:
        assert save_name is not None, "No info about where to save provided"
        save_path = os.path.join(os.getcwd(), save_name)
    return save_path


def export_layer(layer, save_path=None, save_name=None, rewrite=True):
    """
        Save layer
    """
    options = QgsVectorFileWriter.SaveVectorOptions()
    options.driverName = "ESRI Shapefile"
    save_path = get_save_path(save_path=save_path, save_name=save_name)
    if os.path.exists(save_path) and not rewrite:
        print(f"File exists {save_path} and not allowed to rewrite by this program")
        return 1
    else:
        file_list = glob.glob(save_path.rstrip(save_path.split(".")[-1]))
        # Iterate over the list of filepaths & remove each file.
        for file_path in file_list:
            try:
                os.remove(file_path)
            except:
                print("Error while deleting file : ", file_path)
        error = QgsVectorFileWriter.writeAsVectorFormatV2(layer, save_path,
                                                          QgsCoordinateTransformContext(), options,
                                                          # onlySelected=True,
                                                          )
        if error[0] != 0:
            print(f"Error when saving {error}")
        else:
            print(f"Saved file {save_path}")
        return error


def merge_two_vector_layers(layer, layer_add, save_flag=False, save_path=None, save_name=None):
    """
        Add features from layer_add to layer
    """
    prov = layer.dataProvider()
    # add selected features
    for feat in layer_add.getFeatures():
        prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    if save_flag:
        err_save = export_layer(layer, save_path=save_path, save_name=save_name)
    return layer


def create_layer_from_geom(geom, layer_type, geom_type):
    """
        Create vector layer from its geometry object
    """
    feat = QgsFeature()
    feat.setGeometry(geom)
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    return layer


def layer_filter_from_expression(path_to_tiles_layer, expr=None, crs_name="epsg:6933",
                                 save_flag=False, save_path=None, save_name=None,
                                 get_extent=False, ):
    """
        Get layer from the tiles selected from another vector layer using the expression
    """
    tiles_layer = load_layer(path_to_tiles_layer, name="tiles", )
    # select tiles by NUTS/COMM_ID
    selected_tiles = tiles_layer.getFeatures(QgsFeatureRequest().setFilterExpression(expr))
    # print([field.name() for field in tiles_layer.fields()])  # print fields' names

    '''
    save selected tiles to the new layer
    '''
    geom_type = "multipolygon"
    layer_type = geom_type + '?crs=' + crs_name
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    # get fields of the old layer
    inFields = tiles_layer.dataProvider().fields()
    # make fields of the new layer the same as the old one
    layer.startEditing()
    layer.dataProvider().addAttributes(inFields.toList())
    layer.commitChanges()
    # add selected features
    for feat in selected_tiles:
        prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    # crs = tiles_layer.crs()
    # logging.info(f"Used CRS: {crs.description()}")
    # crs_name = crs.authid()
    if save_flag:
        err_save = export_layer(layer, save_path=save_path, save_name=save_name)
    extents = None
    if get_extent:
        ext = layer.extent()
        extents = (ext.xMinimum(), ext.xMaximum(),
                   ext.yMinimum(), ext.yMaximum())
    return layer, extents, save_path  # {"layer": layer, "extent": extent, "save_path": save_path}


def create_pixel_area(i, j, geom_type="polygon",  # either "polygon" or "point"
                      tile_size_x=1, tile_size_y=1,
                      crs_source_name="epsg:4326", transform=False, crs_dest_name="epsg:6933",
                      get_area_only=False, metric=None, save_flag=False, save_name=None,
                      step=0.004166666700000000098, x0=-180.0020833333499866, y0=75.0020833333500008):
    """
        Create pixel quadratic area or point in the center of pixel given its 'raster coordinates'
    """
    # TODO change hardcoded sizes (step, x0, y0) to inferred
    # parameters of the raster layer
    x2 = 180.0020862133499975
    y2 = -65.0020844533500082
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
    elif geom_type == "polygon":
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
                return metric.convertAreaMeasurement(metric.measureArea(feat.geometry()),
                                                     QgsUnitTypes.AreaSquareKilometers)
    else:
        raise NotImplementedError(f"Not known type {geom_type}")
    if transform:
        layer_type = geom_type + '?crs=' + crs_dest_name
    else:
        layer_type = geom_type + '?crs=' + crs_source_name
    layer = create_layer_from_geom(geom, layer_type, geom_type)
    if save_flag:
        if save_name is None:
            save_name = "_".join([geom_type, str(i), str(j)]) + ".shp"
        save_path = os.path.join(os.getcwd(), save_name)
        err_save = export_layer(layer, save_path)
    return layer


def expression_from_nuts_comm(nuts_yes=[], nuts_no=[], comm_yes=[], comm_no=[], comm_exact_yes=[], comm_exact_no=[]):
    """
        construct QGIS SQL-like expression to choose tiles
    """
    # TODO rewrite more beautifully
    att_name = "NUTS_CODE"
    expr = " OR ".join(map(lambda x: "\"" + att_name + "\" LIKE '" + x + "%'", nuts_yes))
    if expr and nuts_no:
        expr += " AND "
    if nuts_no:
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" NOT LIKE '" + x + "%'", nuts_no))
    att_name = "COMM_ID"
    if expr and (comm_yes or comm_no or comm_exact_yes or comm_exact_no):
        expr += " OR "
    expr1 = " OR ".join(map(lambda x: "\"" + att_name + "\" = '" + x + "'", comm_exact_yes))
    if expr1:
        expr += "(" + expr1 + ")"
    if comm_exact_no:
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" <> '" + x + "'", comm_exact_no))
    if expr and (comm_yes or comm_no):
        expr += " OR "
    expr += " AND ".join(map(lambda x: "\"" + att_name + "\" LIKE '" + x + "%'", comm_yes))
    if expr and comm_no:
        expr += " AND "
    if comm_no:
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" NOT LIKE '" + x + "%'", comm_no))
    return expr


def get_sizes_in_pixels_from_degrees(extents,  # xmin, xmax, ymin, ymax
                                     biggest_pixel=40, step=0.004166666700000000098,
                                     x0=-180.0020833333499866, y0=75.0020833333500008):
    """
        Conversion from degrees to pixels with some gap to be sure
    """
    pixel_extents = [math.floor((extents[0] - x0) / step) - 1,
                     math.ceil((extents[1] - x0) / step) + 1,
                     math.floor((y0 - extents[3]) / step) - 1,
                     math.ceil((y0 - extents[2]) / step) + 1]
    return pixel_extents


def delete_layers():
    """
        Delete (hopefully) all the layers in the project
    """
    if 1:
        for l in QgsProject.instance().mapLayers().values():
            QgsProject.instance().removeMapLayer(l.id())
        for l in QgsProject.instance().mapLayers().values():
            print(l.name())
    return 0


def debug_output_with_time(st, ts, logger):
    logger.debug(st)
    ts.append(time.time())
    logger.debug(ts[-1] - ts[-2])


def get_intersect_ids_and_areas(i, j, tiles, result_name, code, global_count,
                                tile_size_x=None, tile_size_y=None,
                                metric=None,
                                eps=0.01 / 100.0,  # relative error allowed for the check of the whole area in intersect
                                level=None, pixel_sizes=None, check_intersection=True, logger=None,
                                path_to_gdal=None):
    """
        For pixels in subarea of raster get IDs of intersecting tiles-municipalities
        and areas of their intersections
    """
    logger.debug("Processing i={} j={} size={}".format(i, j, tile_size_x))
    # print("Processing i={} j={} size={}".format(i, j, tile_size_x))
    ts = [time.time()]
    # get one square area from raster by its i and j pixel coordinates and sizes
    pixel_polygon = create_pixel_area(i, j, tile_size_x=tile_size_x, tile_size_y=tile_size_y,
                                      metric=metric, geom_type="polygon", )

    debug_output_with_time("Polygonized area", ts, logger)

    if not check_intersection:
        # get area of the pixel only, without intersection
        for elem in pixel_polygon.getFeatures():
            area = metric.convertAreaMeasurement(metric.measureArea(elem.geometry()), QgsUnitTypes.AreaSquareKilometers)
            return [[i, j, area]]
    else:
        # intersect polygonized pixel with tiles
        params = {
            "INPUT": tiles,
            "INTERSECT": pixel_polygon,
            'OUTPUT': 'memory:buffer',  # tiles_intersect,  #
            "PREDICATE": [0]
        }
        tiles_intersect = \
            processing.run('qgis:extractbylocation', params,
                           )["OUTPUT"]

        debug_output_with_time("Found intersection tiles", ts, logger)
        # get intersections tiles
        params = {
            "INPUT": tiles_intersect,
            "OVERLAY": pixel_polygon,
            "OUTPUT": 'memory:buffer'
        }
        layer_intersect = \
            processing.run("qgis:intersection", params,
                           )["OUTPUT"]

        debug_output_with_time("Intersected tiles with pixel", ts, logger)

        # get IDs of intersection tiles
        feature_ids = [feature["COMM_ID"] for feature in layer_intersect.getFeatures()]
        feature_nuts = [feature["NUTS_CODE"] for feature in layer_intersect.getFeatures()]
        rows = []  # result accumulation
        # if there is some intersection with municipalities' tiles
        if len(feature_ids) > 0:
            # get area of (large) pixel
            area_total = None
            for ind, elem in enumerate(pixel_polygon.getFeatures()):
                area_total = metric.convertAreaMeasurement(metric.measureArea(elem.geometry()),
                                                           QgsUnitTypes.AreaSquareKilometers)
                assert ind == 0  # only one feature in pixel
            # single pixel or aggregation of pixels?
            if tile_size_x != 1:
                # delete_layers()
                # for case of one intersection check that it's of our code
                f = feature_nuts[0][0:2] == code
                # get area of the first intersection tile
                area_first = None
                for k, feature in enumerate(layer_intersect.getFeatures()):
                    area_first = metric.convertAreaMeasurement(metric.measureArea(feature.geometry()),
                                                               QgsUnitTypes.AreaSquareKilometers)
                    break
                # is the whole area inside one municipality of our country or not
                # yes
                if len(feature_ids) == 1 and f and abs(area_first - area_total) / area_total < eps:
                    # get area of every pixel inside and append to results
                    for x in range(0, tile_size_x):
                        for y in range(0, tile_size_y):
                            rows.append([i + x, j + y, feature_nuts[0], feature_ids[0],
                                         "{:.6f}".format(create_pixel_area(i + x, j + y, geom_type="polygon",
                                                                           tile_size_x=1, tile_size_y=1,
                                                                           get_area_only=True, metric=metric)),
                                         100.0])
                    debug_output_with_time(f"Done with area of size {tile_size_x}", ts, logger)
                    global_count += tile_size_x * tile_size_y
                # if area is divided between municipalities
                else:
                    n = pixel_sizes[level] // pixel_sizes[level + 1]
                    size = pixel_sizes[level + 1]  # current size of "aggregated pixel"
                    for k in range(n):
                        for m in range(n):
                            logger.debug(f"{i} {j} {size} {k} {m}")
                            global_count = get_intersect_ids_and_areas(i + k * size, j + m * size,
                                                                       tiles, result_name, code,
                                                                       global_count,
                                                                       tile_size_x=size, tile_size_y=size,
                                                                       metric=metric,
                                                                       level=level + 1, pixel_sizes=pixel_sizes,
                                                                       check_intersection=True,
                                                                       logger=logger,
                                                                       path_to_gdal=path_to_gdal)
            # single pixel
            else:  # tile_size_x == 1
                areas = []
                ids = []
                ks = []
                # account for possible repetitions of the same tile
                for k, feature in enumerate(layer_intersect.getFeatures()):
                    id = feature["COMM_ID"]
                    nuts_curr = feature["NUTS_CODE"]
                    if (id not in ids) and (nuts_curr[0:2] == code):  # check that part is from this country + not seen
                        ids.append(id)
                        ks.append(k)
                        # get area of intersection in km^2
                        areas.append(
                            metric.convertAreaMeasurement(metric.measureArea(feature.geometry()),
                                                          QgsUnitTypes.AreaSquareKilometers))
                # form results
                for ind in range(len(ids)):
                    k = ks[ind]
                    rows.append([i, j, feature_nuts[k], feature_ids[k],
                                 "{:.6f}".format(areas[ind]), "{:.4f}".format((areas[ind] / area_total) * 100.0)])
                debug_output_with_time("Calculated area of intersections", ts, logger)
                global_count += 1
            # dump obtained results
            with open(result_name, "a+", newline="") as file:
                filewriter = csv.writer(file)
                for row in rows:
                    if row:
                        filewriter.writerow(row)
                        logger.debug(row)
            debug_output_with_time("Dumped results", ts, logger)

            # remove loaded layers
            # delete_layers()
            # gc.collect()
            debug_output_with_time("Deleted all layers", ts, logger)

        return global_count


def get_border_of_area(path_to_tiles_layer, expr_in, expr_out, crs_name,
                       save_flag=False, save_path=None, save_name=None, rewrite=False):
    layer_country, _, _ = layer_filter_from_expression(path_to_tiles_layer, expr=expr_in,
                                                       crs_name=crs_name,
                                                       # save_flag=True, save_name="1.shp"
                                                       )
    layer_outside_country, _, _ = layer_filter_from_expression(path_to_tiles_layer, expr=expr_out,
                                                               crs_name=crs_name,
                                                               #        save_flag=True, save_name="2.shp"
                                                               )
    params = {'INPUT': layer_outside_country,
              'INTERSECT': layer_country,
              'OUTPUT': 'TEMPORARY_OUTPUT',
              'PREDICATE': [4]}  # touches
    tiles_border = \
        processing.run('qgis:extractbylocation', params,
                       )["OUTPUT"]

    err_save = None
    if save_flag:
        err_save = export_layer(tiles_border, save_path=save_path, save_name=save_name, rewrite=rewrite)
    return tiles_border, save_path, err_save


def get_border_of_country(code, path_to_tiles_layer, save_flag=False, save_path=None, save_name=None,
                          crs_name="epsg:6933", rewrite=False,
                          used_field="nuts"):
    """
        Get layer - bordering to the chosen country code
        :param path_to_tiles_layer: path to the whole layer where to extract tiles from (e.g. the whole world)
    """
    if save_flag:
        save_path = get_save_path(save_path=save_path, save_name=save_name)
        if os.path.exists(save_path) and not rewrite:
            print(f"File exists {save_path} and not allowed to rewrite by this program")
            return 1
    if used_field.lower() == "nuts":
        expr_in = expression_from_nuts_comm(nuts_yes=[code, ])
        expr_out = expression_from_nuts_comm(nuts_no=[code, ])
    elif used_field.lower() == "comm_id":
        expr_in = expression_from_nuts_comm(comm_yes=[code, ])
        expr_out = expression_from_nuts_comm(comm_no=[code, ])
    else:
        raise NotImplementedError(f"Wrong used_field {used_field}")
    # print(expr_in)
    # print(expr_out)
    tiles_border, save_path, err_save = get_border_of_area(path_to_tiles_layer, expr_in, expr_out, crs_name,
                                                           save_flag=save_flag, save_path=save_path,
                                                           save_name=save_name, rewrite=rewrite)
    return tiles_border, save_path, err_save


def measure_dist(i, j, tiles_layer, save_flag=False, save_name=None):
    """
        Measure distance from the center of pixel to the given layer's tiles
    """
    target_layer = create_pixel_area(i, j, geom_type="point", transform=True,
                                     crs_source_name="epsg:4326", crs_dest_name="epsg:6933",
                                     save_flag=save_flag, save_name=save_name, )
    params = {'DISCARD_NONMATCHING': False,
              'FIELDS_TO_COPY': [],
              'INPUT': target_layer,  # point
              'INPUT_2': tiles_layer,  # tiles
              'MAX_DISTANCE': None, 'NEIGHBORS': 1, 'OUTPUT': 'TEMPORARY_OUTPUT', 'PREFIX': ''}

    tiles_joined = processing.run('native:joinbynearest', params,
                                  )["OUTPUT"]
    f = False
    for k, feature in enumerate(tiles_joined.getFeatures()):
        # TODO what if tiles_layer empty or more than one feature here
        if k > 1:
            # TODO maybe write all comm_ids if several close with the same dist?
            print(f"More than one tile in joined {i} {j} {dis} {comm_id}")
            f = True
        dis = feature["distance"]
        comm_id = feature["COMM_ID"]
        nuts_code = feature["NUTS_CODE"]
    if f:
        print(f"More than one tile in joined {i} {j} {dis} {comm_id} {feature['CNTR_CODE']}")  #
    # delete point layer from memory, otherwise they will accumulate there and slow down everything
    QgsProject.instance().removeMapLayer(target_layer.id())
    return [f"{float(dis)/1000:.2f}", comm_id, nuts_code, feature["CNTR_CODE"]]
