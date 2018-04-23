import geopyspark as gps
from pyspark import SparkContext
import osr
from geonotebook.wrappers import TMSRasterData


sc = None
def init_sc():
    global sc
    if not sc:
        conf = gps.geopyspark_conf(appName="geopyspark-example", master="local[*]")
        conf.set(key='spark.ui.enabled', value='true')
        sc = SparkContext(conf=conf)

def map_ndvi(M, img, bounds, crs):
    # Start a spark context if needed
    init_sc()
    # Color ramp for NDVI
    ndvi_breaks_dict = {0.05:0xffffe5aa, 0.1:0xf7fcb9ff, 0.2:0xd9f0a3ff, 0.3:0xaddd8eff, 0.4:0x78c679ff, 0.5:0x41ab5dff, 0.6:0x238443ff, 0.7:0x006837ff, 1.0:0x004529ff}
    ndvi_color_map = gps.ColorMap.from_break_map(ndvi_breaks_dict)

    # Convert the CRS into a proj4 string
    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.wkt)
    proj4 = srs.ExportToProj4()


    # Create the projected extent
    projected_extent = gps.ProjectedExtent(gps.Extent(bounds.left,
                                                      bounds.bottom,
                                                      bounds.right,
                                                      bounds.top), proj4=proj4)

    tiles = sc.parallelize([(projected_extent, gps.Tile.from_numpy_array(img, no_data_value=0.0))])
    raster_layer = gps.geotrellis.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, tiles)
    tiled_raster_layer = raster_layer.tile_to_layout(gps.GlobalLayout(),
                                                     target_crs=3857,
                                                     partition_strategy=gps.HashPartitionStrategy(40))
    pyramid = tiled_raster_layer.pyramid(resample_method=gps.ResampleMethod.BILINEAR)



    tms = gps.TMS.build(pyramid, ndvi_color_map)
    M.add_layer(TMSRasterData(tms), name="ndvi")

def reproject_geom(g, proj1, proj2):
    """
    Reprojects a shapely geometry
    """

    import pyproj
    from functools import partial
    from shapely.ops import transform


    project = partial(
        pyproj.transform,
        proj1, # source coordinate system
        proj2) # destination coordinate system

    return transform(project, g)  # apply projection

def show_image(img, cmap='gray'):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(16, 16))
    ax.imshow(img, cmap)
    plt.show()

def show_histogram(arr, no_data_value=None):
    import numpy as np
    import matplotlib.pyplot as plt

    if no_data_value is None:
        flattened = np.ravel(arr)
    else:
        flattened = np.ravel(arr[~(arr == no_data_value)])

    plt.hist(flattened, bins='auto')  # arguments are passed to np.histogram
    plt.title("Histogram with 'auto' bins")
    plt.show()

def compute_ndvi(r, ir):
    import numpy as np

    rf = r.astype(float)
    rf[r == 0] = np.nan
    irf = ir.astype(float)
    irf[ir == 0] = np.nan
    return (irf - rf) / (irf + rf)



# A map of CDL raster values to the category they represent.
cdl_values_to_crops = {0: 'Background',1: 'Corn',2: 'Cotton',3: 'Rice',4: 'Sorghum',5: 'Soybeans',6: 'Sunflower',10: 'Peanuts',11: 'Tobacco',12: 'Sweet Corn',13: 'Pop or Orn Corn',14: 'Mint',21: 'Barley',22: 'Durum Wheat',23: 'Spring Wheat',24: 'Winter Wheat',25: 'Other Small Grains',26: 'Dbl Crop WinWht/Soybeans',27: 'Rye',28: 'Oats',29: 'Millet',30: 'Speltz',31: 'Canola',32: 'Flaxseed',33: 'Safflower',34: 'Rape Seed',35: 'Mustard',36: 'Alfalfa',37: 'Other Hay/Non Alfalfa',38: 'Camelina',39: 'Buckwheat',41: 'Sugarbeets',42: 'Dry Beans',43: 'Potatoes',44: 'Other Crops',45: 'Sugarcane',46: 'Sweet Potatoes',47: 'Misc Vegs & Fruits',48: 'Watermelons',49: 'Onions',50: 'Cucumbers',51: 'Chick Peas',52: 'Lentils',53: 'Peas',54: 'Tomatoes',55: 'Caneberries',56: 'Hops',57: 'Herbs',58: 'Clover/Wildflowers',59: 'Sod/Grass Seed',60: 'Switchgrass',61: 'Fallow/Idle Cropland',63: 'Forest',64: 'Shrubland',65: 'Barren',66: 'Cherries',67: 'Peaches',68: 'Apples',69: 'Grapes',70: 'Christmas Trees',71: 'Other Tree Crops',72: 'Citrus',74: 'Pecans',75: 'Almonds',76: 'Walnuts',77: 'Pears',81: 'Clouds/No Data',82: 'Developed',83: 'Water',87: 'Wetlands',88: 'Nonag/Undefined',92: 'Aquaculture',111: 'Open Water',112: 'Perennial Ice/Snow ',121: 'Developed/Open Space',122: 'Developed/Low Intensity',123: 'Developed/Med Intensity',124: 'Developed/High Intensity',131: 'Barren',141: 'Deciduous Forest',142: 'Evergreen Forest',143: 'Mixed Forest',152: 'Shrubland',176: 'Grassland/Pasture',190: 'Woody Wetlands',195: 'Herbaceous Wetlands',204: 'Pistachios',205: 'Triticale',206: 'Carrots',207: 'Asparagus',208: 'Garlic',209: 'Cantaloupes',210: 'Prunes',211: 'Olives',212: 'Oranges',213: 'Honeydew Melons',214: 'Broccoli',216: 'Peppers',217: 'Pomegranates',218: 'Nectarines',219: 'Greens',220: 'Plums',221: 'Strawberries',222: 'Squash',223: 'Apricots',224: 'Vetch',225: 'Dbl Crop WinWht/Corn',226: 'Dbl Crop Oats/Corn',227: 'Lettuce',229: 'Pumpkins',230: 'Dbl Crop Lettuce/Durum Wht',231: 'Dbl Crop Lettuce/Cantaloupe',232: 'Dbl Crop Lettuce/Cotton',233: 'Dbl Crop Lettuce/Barley',234: 'Dbl Crop Durum Wht/Sorghum',235: 'Dbl Crop Barley/Sorghum',236: 'Dbl Crop WinWht/Sorghum',237: 'Dbl Crop Barley/Corn',238: 'Dbl Crop WinWht/Cotton',239: 'Dbl Crop Soybeans/Cotton',240: 'Dbl Crop Soybeans/Oats',241: 'Dbl Crop Corn/Soybeans',242: 'Blueberries',243: 'Cabbage',244: 'Cauliflower',245: 'Celery',246: 'Radishes',247: 'Turnips',248: 'Eggplants',249: 'Gourds',250: 'Cranberries',254: 'Dbl Crop Barley/Soybeans'}

# A reverse map of above, allowing you to lookup CDL values from category name.
crops_to_cdl_values = {v: k for k, v in cdl_values_to_crops.items()}
