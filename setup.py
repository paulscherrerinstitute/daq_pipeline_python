from setuptools import setup

setup(name="daq_pipeline_python",
      version="0.0.1",
      maintainer="Paul Scherrer Institute",
      maintainer_email="daq@psi.ch",
      author="Paul Scherrer Institute",
      author_email="daq@psi.ch",
      description="Python based DAQ pipeline test.",
      license="GPL3",
      packages=['daq_pipeline',
                'daq_pipeline.metadata',
                'daq_pipeline.pipeline',
                'daq_pipeline.receiver',
                'daq_pipeline.stats',
                'daq_pipeline.store',
                ]
      )
