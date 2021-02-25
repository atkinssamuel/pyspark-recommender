# PySpark Recommender System
## Project Description
The applications in this project helped me gain practical experience with big-data tools, specifically, PySpark. Through the large-scale collaborative-filtering project and mini-applications implemented in this repo, I was able to gain a solid understanding of data handling and model formulation using PySpark. 

In this repository, I implement simple map-reduce programs for counting odd/even integers, counting words in a text document, and calculating simple properties present in a supplied large data-frame. 

I also implement a large-scale collaborative-filtering recommender system. This system takes in user rating data pertaining to various movie IDs and models the relationship between the users by utilizing the collaborative filtering approach with alternating least squares (ALS). For details about the results and application insights, see the report.pdf file.

## Setup
### Organization
The scripts for each of the PySpark applications are present in the ```scripts/``` folder. The results for each of the implementations are in the ```results/``` folder. These results have also been appended to the end of the README in image format. The outputs from each script have been included in the ```outputs/``` folder. 
### Conda Environment Setup
**Create environment from environment.yml**:  
*From base directory:*  
```conda env create -f ./environment.yml```

**Update environment from environment.yml**:  
*From base directory and after activating existing environment:*  
```conda env update --file ./environment.yml```

### Execution
After creating a conda environment using the supplied ```environment.yml``` file, inspect the files within the ```scripts/``` folder to understand the purpose of each PySpark application. Then, simply run each of the scripts and observe the output. 

*NOTE: the ```recommender_sys.py``` file has a ```question``` flag that enables different branches of the script to run. Be sure to set this flag if you wish to control the flow of the script*
