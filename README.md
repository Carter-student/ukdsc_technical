# UKDSC Technical Repo

Welcome! To run this code you are going to want to create a config in the config folder called config.yaml in the style of the below for the database we are connecting to.
```
host: !!str USE_DB_ADDRESS
port: !!str USE_DB_PORT
dbname: !!str USE_DB_NAME
username: !!str YOUR_USERNAME_HERE
password: !!str YOUR_PASSWORD_HERE
```

Then pip install the requirements.txt. I used Python3.10.10 so this is recommended.

```
pip install -r requirements.txt
```

You can then run the main.py file directly. If you want to use cached data from previous runs instead of connecting each time you run you can set **use_cache** to True in the DataPrep class.

Ignore the table_orms.py file this was only included because I considered using it and I still want to show off that I know how. 