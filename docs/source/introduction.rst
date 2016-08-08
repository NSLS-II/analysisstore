Introduction
============

Overview
--------

The goal of NSLS-II middle-layer and high level controls framework 
is to provide an integrated, 
end-to-end solution for data collection and analysis. 
Services such as, metadataservice, filestore, archiverappliance, 
amostra, conftrak, and Olog provide means to collect and query raw data captured using 
ophyd_ and bluesky_. 

.. _bluesky: http://nsls-ii.github.io/bluesky/ 

.. _ophyd: https://nsls-ii.github.io/ophyd/

The flexible, intuitive nature and smart indexing capabilities of these services
are heavily used and fairly popular. As a result, various users requested their 
an additional service that would allow them to manage their data analysis in a similar fashion.
The goal of analysisstore is to address these needs. It allows users to keep track of their
data analysis data streams (or files if used with filestore).
This documentation provides details about usage and technical background of analysisstore.

.. warning:: This project is in early alpha stage. There might be API changes.

Technology
----------

analysisstore is implemented purely in Python and tested for Python 3.4 and 3.5. It is built
as a web service that utilizes async utilities of tornado and flexibility of MongoDB.
Our exprience indicates this sort of microservice structure is widely acceptable for 
most data analysis applications as the number of clients 
is definitely not web scale. Microservice framework allows us to embed
these services within others and dramatically decreases development time and effort. 
These microservices are also easier to maintain in the long run.

