Document Types
=====

Analysisstore is built on a similar data model to metadatastore_. AnalysisHeader document is the point of entry for creating a trace for any sort of data analysis. One can think of this as the head of the linked-list. 

AnalysisHeader
--------------

In order to start saving a data analysis process and its associated files, an AnalysisHeader document must be created.
This document requires uid, time, and provenance. As in our other services, uid and time are used for uniqueness and 
conveniently pinpointing data. 'provenance' is the dictionary that enforces the place of origin of this data analysis stream.
This simple yet flexible 
name-value structure allows users to keep track of the chronology of analysis performed on a given data stream.

.. _metadatastore: https://nsls-ii.github.io/architecture-overview.html

.. code:: python
        
        {
            "properties": {
                "config": {
                    "type": ["object"],
                    "description": "Meta-data regrading slow-chaning configuration, such as equipment specifications and calibration"
                },
                "provenance": {
                    "type": ["object"],
                    "description": "Meta-data regrading set of tasks that generated this analysis data"
                },

                "project": {
                    "type": "string",
                    "description": "Name of project that this run is part of"
                },
                "sample": {
                    "type": ["string"],
                    "description": "UID to another collection in amostra"
                },
                "beamline_id": {
                    "type": "string",
                    "description": "The beamline ID"
                },
                "scan_id": {
                    "type": "integer",
                    "description": "Scan ID number, not globally unique"
                },
                "time": {
                    "type": "number",
                    "description": "Time the run started.  Unix epoch time"
                },
                "uid": {
                    "type": "string",
                    "description": "Globally unique ID for tihs run"
                },
                "group": {
                    "type": "string",
                    "description": "Unix group to associate this data with"
                },
                "owner": {
                    "type": "string",
                    "description": "Unix owner to associate this data with"
                }
            },
            "required": [
                "uid",
                "time",
                "provenance"
             ],
            "type": "object",
            "description": "Header of the analysis data. No real data in this header but metadata that allows us locate data"
        }


DataReferenceHeader
--------------------

Some sort of meaningful information must be kept in order for data stream to be retrieved/processed.
The flexible documents allow any metadata as a means of convenience, however additional information
such as shape, source, data type, units, etc. of a data stream are essential to data analysis.
In order to enforce this, analysisstore introduces DataReferenceHeader.
As each DataReferenceDocument acts as a header for a data stream, a data_keys field is introduced, providing
users a convenient way to store information mentioned above. In addition to data_keys, uid, analysis_header(foreign_key), and time
are also required.
Given a single AnalysisHeader, users can define multiple DataReferenceHeaders that describe the contents of 
multiple data streams.

.. code:: python


         {
                "definitions": {
                    "data_key": {
                        "title": "data_key",
                        "description": "Describes the objects in the data property of data_reference documents",
                        "properties": {
                            "dtype": {
                                "enum": [
                                    "string",
                                    "number",
                                    "array",
                                    "boolean",
                                    "integer"
                                ],
                                "type": "string",
                                "description": "The type of the data in the data_reference."
                            },
                            "external": {
                                "pattern": "^[A-Z]+:?",
                                "type": "string",
                                "description": "Where the data is stored if it is stored external to the data_references."
                            },
                            "shape": {
                                "items": {
                                    "type": "integer"
                                },
                                "description": "The shape of the data.  Null and empty list mean scalar data."
                            },
                            "source": {
                                "type": "string",
                                "description": "The source (ex piece of hardware) of the data."
                            }
                        },
                        "required": [
                            "source",
                            "dtype",
                            "shape"
                        ],
                        "type": "object"
                    }
                },
                "properties": {
                    "data_keys": {
                        "additionalProperties": {
                            "$ref": "#/definitions/data_key"
                        },
                        "type": "object",
                        "description": "The describes the data to be in the data_reference Documents",
                        "title": "data_keys"
                    },
                    "uid": {
                        "type": "string",
                        "description": "Globally unique ID for this data_reference descriptor.",
                        "title": "uid"
                    },
                    "analysis_header": {
                        "type": "string",
                        "description": "Globally unique ID to the analysis_header document this descriptor is associtaed with."
                    },
                    "time": {
                        "type": "number",
                        "description": "Creation time of the document as unix epoch time."
                    }
                },
                "required": [
                    "uid",
                    "data_keys",
                    "analysis_header",
                    "time"
                ],
                "type": "object",
                "title": "data_reference_header",
                "description": "Document to describe the data captured in the associated data_reference documents"
            }


DataReference
--------------

Each data reference stands for a single point in the data stream. This point is not unique to a single data_key but
collection of data_keys. In other words, a DataReference document contains multiple data points and their corresponding
timestamps. DataReference documents can contain links to filestore as well as other services.

.. code:: python
      
        {
        "properties": {
                "data": {
                    "type": "object",
                    "description": "The actual analyzed data"
                },
                "timestamps": {
                    "type": "object",
                    "description": "The timestamps of the individual analyzed data"
                },
                "data_reference_header": {
                    "type": "string",
                    "description": "UID to point back to Descriptor for this data_reference stream"
                },
                "seq_num": {
                    "type": "integer",
                    "description": "Sequence number to identify the location of this data_reference in the data_reference stream"
                },
                "time": {
                    "type": "number",
                    "description": "The data_reference time.  This maybe different than the timestamps on each of the data entries"
                },
                "uid": {
                    "type": "string",
                    "description": "Globally unique identifier for this data_reference"
                }
            },
            "required": [
                "uid",
                "data",
                "timestamps",
                "time",
                "data_reference_header",
                "seq_num"
            ],
            "additionalProperties": false,
            "type": "object",
            "title": "data_reference",
            "description": "Document to record a quanta of collected data"
         }

AnalysisTail
------------

Once data analysis is concluded, AnalysisTail is required in order to successfully locate the set of data analysis
documents mentioned earlier. The purpose of this document is to simply state the success/failure of the data analysis.
In addition to this, any sort of metadata can be stored within this document, in case user would like to keep for future
analysis purposes.

.. code:: python

       {
            "properties": {
                "analysis_header": {
                    "type": "string",
                    "description": "Reference back to the analysis_header document that this document is paired with."
                },
                "reason": {
                    "type": "string",
                    "description": "Long-form description of why the run ended"
                },
                "time": {
                    "type": "number",
                    "description": "The time the run ended. Unix epoch"
                },
                "analysis_status": {
                    "type": "string",
                    "enum": ["final", "raw"],
                    "description": "State of the analysis when it ended"
                },
                "uid": {
                    "type": "string",
                    "description": "Globally unique ID for tihs run"
                }
            },
            "required": [
                "uid",
                "analysis_header",
                "time",
                "exit_status"
            ],
            "type": "object",
            "description": "Document for the end of a analysis indicating the success/fail state of the run and the end time"
        }
