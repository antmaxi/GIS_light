import os

folder_dist_save = os.path.join(os.getcwd(), "dist", "raw")
folder_tiles = os.path.join(os.getcwd(), "pixel", "tiles")
folder_labels = os.path.join(os.getcwd(), "label", )
dist_header = ['X', 'Y', "COUNTRY_CODE", "NUTS_CODE", "COMM_ID",
               'DIST_LOCKDOWN_KM', "NEAREST_COMM_ID", "NEAREST_NUTS", "NEAREST_COUNTRY_CODE"]
