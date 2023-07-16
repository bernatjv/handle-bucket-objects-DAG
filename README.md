# handle-bucket-objects-DAG
The Python script `handle_bucket_objects.py` defines an Airflow DAG in charge of a simple real use case example with functionalities such as user defined Macros, fields supporting Jinja templates, XComs parameters and deferrable operators.

Its tasks consist of sensing new objects starting with the prefix of your choice in a GCS bucket using `GCSObjectsWithPrefixExistenceSensor`, and locating the CSV and TXT ones to be processed in whichever way is desired. For example in this case, the third task of the DAG `copy_txt_to_other_dir` simply copies all TXT files from the indicated bucket into the `/data/copy` directory of the same bucket, where they can be used for further purposes. This is done by calling the `scripts/copy_files.sh` bash script passing it the proper arguments. The directories of the filesystem are named according to Google Cloud Composer environments architecture, since that is the optimal kind of environment to run this DAG.
