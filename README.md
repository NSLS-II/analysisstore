# analysisstore
Prototype analysis results database


# Analysis database proposal

## Type
```
- uid: str (unique identifier for this document)
- name: str (unique name for this result type)
- description: str, describe this result type  # or is name sufficient?
- version: str  # or maybe that could just be glommed into the name?
- validation_info?:  some sort of dict/list, optional
- templates:  dict, optional  # for web display of result types
  - [type](eg. small/large/oneline?): [template](str)
```

## Conf
```
- uid: str (unique identifier for this document)
- executable: str
- executable_version: str
- args_to_executable: list
- other_inputs: dict  # capture input and config files, and?
- env: dict, os.environ, optional
- conda_list_e: str (output of subprocess.check_output(['conda', 'list', '-e']).decode()), optional
- full_analysis_script: str, full copy of the analysis script (optional)
```

## Header
```
- uid: str (unique identifier for this document)
- analysis_type: uid  # or {uid, source_name, url} ref dict into Type
- parents: list of dicts  # just data? or config/input files? which is which?
  - uid: str (relevant uid for the parent data)
  - source_name: str (database name of original data)
  - url: str (database url)
- timestamp (optional)
- analysis_conf: uid  # or {uid, source_name, url} ref dict into Conf
- conf_overrides:  dict, optional  # means to record per run arg/input/config changes, optional
```

## Results
```
- header_uid: str, reference to an AnalysisHeader
- timestamp (optional, if diff from header timestamp and same for all keys)
- key1:
  - value: scalar, string, dict, remote {uid: str, source_name: str, url: str}
  - description: describe what is in key1
  - timestamp (optional, if diff from timestamp for other keys)
- key2:
  - value: scalar, string, dict, remote {uid: str, source_name: str, url: str}
  - description: describe what is in key2
  - timestamp (optional, if diff from timestamp for other keys)
```
