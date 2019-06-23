For running the model algorithm, in the root folder just run:

``` sh
    python trainer/task.py --input_train_data_path=files path where train data is saved \
                           --browse_score=float, such as 0.5 \
                           --basket_score=float \
                           --output_filename=where to export matrix
```


It's possible to export the job to ML-Engine:

 ```sh
    gcloud ai-platform jobs submit training UNIQUE_JOB_NAME --region=us-east1 \
                                                            --staging-bucket=gs://papis19wjf \
                                                            --job-dir=gs://papis19wjf \
                                                            --package-path=trainer \
                                                            --module-name=trainer.task \
                                                            --config=trainer/config.yaml \
                                                            -- \
                                                            --input_train_data_path=gs://papis19wjf/output_train* \
                                                            --browse_score=0.5 \
                                                            --basket_score=2.5 \
                                                            --output_filename=gs://papis19wjf/trainer/sim_matrix
```
