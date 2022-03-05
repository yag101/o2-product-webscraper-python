# o2-product-webscraper-python
A simple web scrape of the O2 phone product page, bundled with an Airflow operator.

I have stored the web scrape as iPython notebook file so that the code can be easily explored.
Please refer to the file 'O2-web-scrape.ipynb' for this.

The Airflow operator can easily be integrated into an Airflow DAG with a connection to a GCP environment - it accepts parameters for the name of the bucket you wish to store the files in, and also the name you wish to give the file that it will upload.

Below is a very simple planned Airflow architecture diagram.

![image](https://user-images.githubusercontent.com/12210480/156880547-1db3d685-d0fa-4191-8740-aa840f58d665.png)
