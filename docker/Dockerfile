FROM continuumio/miniconda3:4.6.14
RUN conda install -c conda-forge cassandra-driver && conda install numpy && ln -s /opt/conda/lib/python3.7/site-packages/daq_pipeline/start_simulated_pipeline.py /start_simulated_pipeline.py

COPY docker/sources.json /sources.json
ADD build/lib /opt/conda/lib/python3.7/site-packages/