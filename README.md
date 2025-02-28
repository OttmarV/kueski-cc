# kueski-cc
You can find the script named as kuesi-cc.py

To execute this script through Databricks:

1. Create an account for Databricks Community Edition.

2. Click on Compute at the left panel and add a name for your cluster (since this is free, there is only one option to choose). Click on Libraries tab and click on Install New. Select PyPi and install kagglehub. 

3. Once the cluster gets created (takes around 5 mins), go to workspace tab, click Home folder and create your own folder.

4. Inside the folder, click on Create dropdown at the top right corner and then click Notebook.

5. Execute the following commands, one per cell:

import kagglehub
# Download latest version of dataset
path = kagglehub.dataset_download("ehallmar/daily-historical-stock-prices-1970-2018")
print("Path to dataset files:", path)


# Verified the dataset was downloaded correctly
!ls -la /root/.cache/kagglehub/datasets/ehallmar/daily-historical-stock-prices-1970-2018/versions/1/historical_stock_prices.csv


# Move file to DBFS in databricks
dbutils.fs.mv("file:/root/.cache/kagglehub/datasets/ehallmar/daily-historical-stock-prices-1970-2018/versions/1/historical_stock_prices.csv", "dbfs:/FileStore/tables/historical_stock_prices.csv")

# Validate the csv file was moved successfully using dbutils
dbutils.fs.ls("dbfs:/FileStore/tables/historical_stock_prices.csv")

6. Now you can copy and paste the kueski-cc.py script into a single cell, uncomment the df.show(10) command so you are able to see the results displayed and finally, execute the cell. 

