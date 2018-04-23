FROM quay.io/lossyrob/jupyter-geopyspark:musa

USER root

# rio-color is a library for color correcting satellite imagery
RUN pip install rio-color

# rasterio[s3] adds on the ability to read from AWS S3
RUN pip install rasterio[s3]

# imageio is a library for reading and writing images
RUN pip install imageio

# sklearn is a machine learning library
RUN pip install scikit-learn

USER hadoop
