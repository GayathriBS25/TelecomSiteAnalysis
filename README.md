# TelecomSiteAnalysis
This project processes daily telecom network data from multiple sources, including GSM, UMTS, LTE, and site information.

Task is configured and run in Jupyter. The input is fetched from a Git repository and the output is saved in PostgreSQL.

The data is undergone null checks to avoid irregular/wrong outputs.
Data was seperated using ';', rather than ',', which caused parsing issue in the beginning. 

The application can be deployed in Airflow. 
  For this purpose I have created a etl_dag.py file and a telecom_site_analysis.py script seperately to avoid any ogic issues during refactoring of the code in future.
  The database details, git url can be moved to a configuration file to use globally, by only changing the configuration file.
  The code can be sceduled to run daily(@daily) using Airflow. 
  Retries can be increased to avoid temporary issues like network failures during the running of dag.  

  Since Git is used, version control is ensured, as code changes can be tracked and rolledback if necessary.

  

  
  
  
