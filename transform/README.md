This folder holds files responsible for reading data from main datasource (BigQuery in our case) and preparing it for later usage in model training.

For running DirectRunner example:

```
export PYTHONPATH=. && python preprocess.py --input_sql=transform/retrieve_data.sql \
                                            --train_init_date=2017-01-01 \
                                            --train_end_date=2017-01-01 \
                                            --test_init_date=2017-01-02 \
                                            --test_end_date=2017-01-02 \
                                            --project=dafiti-analytics \
                                            --temp_location=/tmp/papis19/t1 \
                                            --max_num_workers=1 \
                                            --staging_location=/tmp/papis19/t1 \
                                            --tft_temp=/tmp/papis19/t1 \
                                            --tft_transform=/tmp/papis19/t1 \
                                            --nitems_filename=/tmp/papis19/nitems \
                                            --output_train_filename=/tmp/papis19/output_train \
                                            --output_test_filename=/tmp/papis19/output_test
```



For running DataflowRunner, cd into `transform` folder and run:

```
export python main.py --input_sql=preprocess/retrieve_data.sql \
                      --train_init_date=2017-01-01 \
                      --train_end_date=2017-01-01 \
                      --test_init_date=2017-01-02 \
                      --test_end_date=2017-01-02 \
                      --project=dafiti-analytics \
                      --temp_location=gs://papis19wjf/temp \
                      --max_num_workers=10 \
                      --staging_location=gs://papis19wjf/staging \
                      --tft_temp=gs://papis19wjf/tft_temp \
                      --tft_transform=gs://papis19wjf/tft_transform \
                      --nitems_filename=gs://papis19wjf/nitems \
                      --output_train_filename=gs://papis19wjf/output_train \
                      --output_test_filename=gs://papis19wjf/output_test \
                      --job_name=papis19 \
                      --runner=DataflowRunner
```
