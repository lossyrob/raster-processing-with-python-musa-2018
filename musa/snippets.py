def reproject_geom(g, proj1, proj2):
    """
    Reprojects a shapely geometry
    """
    from functools import partial
    import pyproj
    from shapely.ops import transform

    project = partial(
        pyproj.transform,
        proj1, # source coordinate system
        proj2) # destination coordinate system

    return transform(project, g)  # apply projection

def bounds_to_polygon(bounds):
    """
    Converts a rasterio bounds to a shapely polygon
    """
    from shapely.geometry import Polygon

    return Polygon([(bounds.left, bounds.bottom),
                    (bounds.left, bounds.top),
                    (bounds.right, bounds.top),
                    (bounds.right, bounds.bottom),
                    (bounds.left, bounds.bottom)])
