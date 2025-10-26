# Basic Overview of Data pipeline using mysql

### Description:
In this Project I wanted to create a basic Data pipeline using mysql . For this project I am collecting Data using Kaggle api. We nedd to mention the dataset name and storage path and the pipeline fetches the data using api and download's it. Then a connection is made to mysql database with the provided credential's. Then it should go through a data cleaning process but it's not implemented yet will do it later. Then it will pass through data ingestion step where for each csv file's downloaded it creates a table using the file name of the csv file and the column names are fetched using the first row of csv file. Then the values are ingested in batches. Also i've implemented extensive logging for the pipeline. 

---

### Diagram:

---

### Setup:

If you want to reproduce it follow the steps.

- Git clone the project into your local system
- Create a Python Environment inside the Project Directory:
  - For windows:

    `Python -m venv venv`
    
    `venv\Scripts\activate`
  - For linux:
    
    `Python3 -m venv venv `

    `Source venv/bin/activate`
- Then in terminal use requirement's.txt to install necesarry packages
  
  `pip install -r requirement's.txt`

- Create a .env file in the root directory

    ```
    DB_USER=
    DB_PASSWORD=
    DB_HOST=
    DB_PORT=
    DB_NAME=
    ```
- Insert your database credential's in the .env file

- In the main.py file write the kaggle dataset name and the output folder

- Now run the main.py file


---

## To-do:

- [ ] Support for more data source

- [ ] Support for more data types

- [ ] Better Logging

- [ ] Implemeting Schema Extractor

- [ ] Validation before Insertion
---