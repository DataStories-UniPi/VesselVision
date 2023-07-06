# VesselVision
### Source code for the "VesselVision - Fleet Safety Awareness over Streaming Vessel Trajectories" demonstration paper


# Installation 
In order to use VesselVision in your project, download all necessary modules in your directory of choice via pip or conda, and install their corresponding dependencies, as the following commands suggest:

```Python
# Using pip/virtualenv
pip install −r requirements.txt

# Using conda
conda install --file requirements.txt
```


## Usage
To use VesselVision at first run the ```MaSEC.py``` script using the following command
``` Python
python VesselVision.py
```

which will initialize Zookeeper and Apache Kafka and create the Topics needed for the data stream and its results. Afterwards, in order to instantiate the web application (```monitor```) for the visualization, run the following command
``` Python
python -m bokeh serve --show ./monitor --allow-websocket-origin=<SERVER_IP>
```

Adjusting the parameters of VesselVision is possible via the ```lib/kafka_config_c_p_v01.py``` file.



# Contributors
Andreas Tritsarolis; Department of Informatics, University of Piraeus

Nikos Pelekis; Department of Statistics & Insurance Science, University of Piraeus

Yannis Theodoridis; Department of Informatics, University of Piraeus


# Acknowledgement
This work was supported in part by the Horizon Framework Programmes of the European Union under grant agreements No 957237 (VesselAI; https://vessel-ai.eu) and No 101070279 (MobiSpaces; https://mobispaces.eu).
