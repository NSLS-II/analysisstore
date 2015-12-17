# analysisstore
Prototype analysis results database


# Analysis database proposal

## Header
```
- uid: str (unique identifier for this document)
- parents: list of dicts
  - uid: str (relevant uid for the parent data)
    source_name: str (database name of original data)
    url: str (database url)
- analysis_info: dict
    executable: str
    executable_version: str
    args_to_executable: list
    conda_list_e: str (output of subprocess.check_output(['conda', 'list', '-e']).decode())
- full_analysis_script: str, full copy of the analysis script (optional)
```
## Log
```
- header_uid: str, reference to an AnalysisHeader
- stdout: full dump of stdout
- stderr: full dump of stderr
```
## Results
```
- header_uid: str, reference to an AnalysisHeader
- key1:
  - value: scalar, uid, small vector
  - descriptor: describe what is in key1
- key2:
  - value: scalar, uid, small vector
  - descriptor: describe what is in key2
```
