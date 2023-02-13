# Python-based ETL of SynPUF data to CDMv5-compatible CSV files

This is an implementation of the SynPUF Extract-Transform-Load (ETL)
specification designed to generate a set of CDMv5-compatible CSV files
that can then be bulk-loaded into your RDBMS of choice.

## Overview of Steps

1. Clone the repo and run the docker container
2. cd into the etl directory
3. Type 'python pipeline.py' to run the ETL - pipeline.py is a script that runs the 3 etl steps, extract.py, transform.py, and load.py
4. Enter answers to the prompts

The Entire pipeline takes about 2 hours to run. If testing, please only select one sample to run.

## What's going on under the hood?
### Install required software

The ETL process comes in a Docker container with all necessary files and packages. To run the ETL, you need to have Docker installed on your machine. You can download Docker from [here](https://www.docker.com/products/docker-desktop).

The original ETL required python 2.7 and the dotenv library, but some code has been updated to be compatible with python 3 and has been tested with python 3.11

### Download SynPUF input data
The SynPUF data is divided into 20 parts (8 files per part), and the files for each part should be saved in respective directories DE_1 through DE_20. They can either be downloaded with a python utility script (``extract.py``) or manually, described in the next two subsections.

#### Download using python script:

In the etl folder, there is a python program 'extract.py', which can be run to fetch one or more of the 20 SynPUF data sets. Run as follows:

``python extract.py path/to/output/directory <SAMPLE> ... [SAMPLE]``

Where each SAMPLE is a number from 1 to 20, representing the 20 parts of the CMS data. If you only wanted to obtain samples 4 and 15, you would run:

``python extract.py path/to/output/directory 4 15``

To obtain all of the data, run:

``python extract.py path/to/output/directory 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20``
OR
``python extract.py path/to/output/directory all``

#### Manual download:
Hyperlinks to the 20 parts can be found here:
<https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html>

For example for DE_1, create a directory called DE_1 and download the following files:

DE1\_0\_2008\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1A.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1B.zip  
DE1\_0\_2008\_to\_2010\_Inpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Outpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Prescription\_Drug\_Events\_Sample\_1.zip  
DE1\_0\_2009\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2010\_Beneficiary\_Summary\_File\_Sample\_1.zip  

Note: - If you are downloading the files manually from CMS website, you need to rename 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.csv.zip'
to 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.zip'.
Also, some zipped files have '.Copy.csv' file inside them. Rename those files from 'Copy.csv' to '.csv' after unzipping the zipped files.
If you use the download script, you don't have to do all of these manual steps. The script will take care of all these.

#### Download CDMv5 Vocabulary files (already added to repo)
Download vocabulary files from <http://www.ohdsi.org/web/athena/>, ensuring that you select at minimum, the following vocabularies:
SNOMED, ICD9CM, ICD9Proc, CPT4, HCPCS, LOINC, RxNorm, and NDC.

- Unzip the resulting .zip file to a directory.
- Because CPT4 vocabulary is not part of CONCEPT.csv file, one must download it with the provided cpt4.jar program via:
``java -Dumls-apikey=<xxx> -jar cpt4.jar 5 <output-file-name>``, which will append the CPT4 concepts to the CONCEPT.csv file. You will need to pass in your UMLS credentials in order for this command to work. Only ``apikey`` and ``5`` are required.
- Note: This command works with Java version 10 or below.

- Note: These files as of 2023-02-07 are available in the /Users/me/Documents/GitHub/dec-etl-project/data/BASE_OMOP_INPUT_DIRECTORY directory.

#### Setup the environment file
Edit the variables in the .env file which specify various directories used during the ETL process.
Environment files have been edited to include appropriate directories in the data folder of the repo.

- BASE\_SYNPUF\_INPUT\_DIRECTORY = where the downloaded CMS directories are contained, that is, the DE\_1 through DE\_20 directories.
- BASE\_OMOP\_INPUT\_DIRECTORY = the CDM v5 vocabulary directory, for example: /workspaces/practice/cms-synpuf/Data/BASE_OMOP_INPUT_DIRECTORY.
- BASE\_OUTPUT\_DIRECTORY = contains all of the output files after running the ETL.
- BASE\_ETL\_CONTROL\_DIRECTORY = contains files used for auto-incrementing record numbers and keeping track of physicians and physician institutions over the 20 parts so that the seperate DE\_1 through DE\_20 directories can be processed sequentially. These files need to be deleted if you want to restart numbering.

## Run ETL on CMS data
To process any of the DE_1 to DE_20 folders, run:

- ``python transform.py <sample number>``
    - Where ``<sample number>`` is the number of one of the samples you downloaded from CMS
    - e.g. ``python transform.py 4`` will run the ETL on the SynPUF data in the DE_4 directory
    - The resulting output files should be suitable for bulk loading into a CDM v5 database.

The runs cannot be done in parallel because counters and unique physician and physician institution providers are detected and carried over multiple runs (saved in BASE\_ETL\_CONTROL\_DIRECTORY). I recommend running them sequentially from 1 through 20 to produce a complete ETL of the approximately 2.33M patients. If you wanted only 1/20th of the data, you could run only sample number 1 and load the resulting .csv files into your database.

Note: - On average, the transform.py program takes approximately 45-60 minutes to process one input file (e.g. DE_1).  We executed the program on an Intel Xeon CPU E3-1271 v3, with 16GB of memory and it took approximately 14 hours to process all 20 DE files.

All the paths are taking from the ``.env`` file that was set up previously.

## Load data into the database
To load the concatenated csv files folders, run:

- ``python load.py``

This will load the data into a DuckDB database specified in the Data folder.


## Open issues and caveats with the ETL
a) As per OHDSI documentation for the [observation](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:observation) and [measurement](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:measurement) tables, the fields 'value_as_string', 'value_as_number', and 'value_as_concept_id' in both tables are not mandatory, but Achilles Heels gives an error when all of these 3 fields are NULL. Achilles Heels requires one of these fields
    should have non-NULL value. So, to fix this error, field 'value_as_concept_id' has been populated with '0' in both the measurement and observation output .csv files.

b) The concepts for Unknown Race, Non-white, and Other Race (8552, 9178, and 8522) have been deprecated, so race_concept_id in Person file has been populated with
    '0' for these deprecated concepts.

c) Only two ethnicity concepts (38003563, 38003564) are available.  38003563: Hispanic and  38003564: Non-Hispanic.

d) When a concept id has no mapping in the CONCEPT_RELATIONSHIP table:
- If there is no mapping from OMOP (ICD9) to OMOP (SNOMED) for an ICD9 concept id, target_concept_id for such ICD9 concept id is populated with '0' .
- If there is no self-mapping from OMOP (HCPCS/CPT4) to OMOP (HCPCS/CPT4) for an HCPCS/CPT4 concept id, target_concept_id for such HCPCS/CPT4 concept id is populated with '0' .
- If there is no mapping from OMOP (NDC) to OMOP (RxNorm) for an NDC concept id, target_concept_id for such NDC concept id is populated with '0'.

e) The source data contains concepts that appear in the CONCEPT.csv file but do not have relationship mappings to target vocabularies. For these, we create records with concept_id 0 and include the source_concept_id in the record. Achilles Heel will give warnings about these concepts for the Condition, Observation, Procedure, and Drug tables as follows. If condition_concept_id or observation_concept_id or procedure_concept_id or drug_concept_id is '0' respectively:
- WARNING: 400-Number of persons with at least one condition occurrence, by condition_concept_id; data with unmapped concepts
- WARNING: 800-Number of persons with at least one observation occurrence, by observation_concept_id; data with unmapped concepts
- WARNING: 600-Number of persons with at least one procedure occurrence, by procedure_concept_id; data with unmapped concepts
- WARNING: 700-Number of persons with at least one drug exposure, by drug_concept_id; data with unmapped concepts

f) About 6% of the records in the drug_exposure file have either days_supply or quantity or both set to '0' (e.g. days_supply = 10 & quantity=0 OR quantity=120 & days_supply=0). Though such
    values are present in the input file, they don't seem to be correct. Because of this, dosage calculations would result in division by zero, hence effective_drug_dose has not been calculated. For that reason we have also left the dose_era table empty. The CMS documentation says the following about both quantity and days_supply, "This variable was imputed/suppressed/coarsened as part of disclosure treatment. Analyses using this variable should be interpreted with caution. Analytic inferences to the Medicare population should not be made when using this variable.""

g) The locations provided in the DE_SynPUF data use [SSA codes](https://www.resdac.org/cms-data/variables/state-code-claim-ssa), and we mapped them to 2-letter state codes. However SSA codes for Puerto Rico('40') and
    Virgin Islands ('48') as well other extra-USA locations have been coded in source and target data as '54' representing Others, where Others= PUERTO RICO, VIRGIN ISLANDS, AFRICA, ASIA OR CALIFORNIA; INSTITUTIONAL PROVIDER OF SERVICES (IPS) ONLY, CANADA & ISLANDS, CENTRAL AMERICA AND WEST INDIES, EUROPE, MEXICO, OCEANIA, PHILIPPINES, SOUTH AMERICA, U.S. POSSESSIONS, AMERICAN SAMOA, GUAM, SAIPAN OR
    NORTHERN MARIANAS, TEXAS; INSTITUTIONAL PROVIDER OF SERVICES (IPS) ONLY, NORTHERN MARIANAS, GUAM, UNKNOWN.

h) As per OMOP CDMv5 [visit_cost](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:visit_cost) documentation, the cost of the visit may contain just board and food,
    but could also include the entire cost of everything that was happening to the patient during the visit. As the input data doesn't have any specific data
    for visit cost, we are not writing any information to visit_cost file.

i) There is no specific data in the input DE_SynPUF files for device cost, so only a header line is written to the device_cost file.

j) Because each person can be covered by up to four payer_plans, we cannot uniquely assign a payer_plan_period_id to drugs or procedures within the drug_cost and procedure_cost files. We leave payer_plan_period_id as blank in those two files.The calculation for the fields payer_plan_period_start_date and payer_plan_period_end_date is based on the values of the following 4 fields of the input beneficiary files: BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS, BENE_HMO_CVRAGE_TOT_MONS, and PLAN_CVRG_MOS_NUM, corresponding to the number of months a beneficiary was covered under each of up to 4 plans (Medicare Part A, Medicare Part B, HMO, and Medicare Part D). Every beneficiary can thus be covered by up to 4 plans, over the three years of data (2008-2010). The CDM requires the specification of start and end date of coverage, which we do not have. Thus we will make some (questionable) assumptions and create payer_plan_period_start_date and payer_plan_period_end_date records for each of the 4 plans using the information about the number of months a beneficiary was covered in a given year as follows:

- if the value of the fields is 12 in 2008, 2009, and 2010, payer_plan_period_start_date is set to '1/1/2008' and payer_plan_period_end_date is set to '12/31/2010'.
- if the value of the fields is 12 in 2008 and 2009 and is less than 12 in 2010, payer_plan_period_start_date is set to '1/1/2008' and payer_plan_period_end_date
  is set to '12/31/2009 + #months in 2010' (12/31/2009 if #months=0).
- if the value of the fields is 12 in 2008 and 2010 and is less than 12 in 2009, different payer_plan_period_start_date and payer_plan_period_end_date are set for
  these 3 years. Three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to: [('1/1/2008', '12/31/2008'),
  ('1/1/2009', '1/1/2009 + #months' no record is written if #months=0), ('1/1/2010', '12/31/2010')]
- if the value of the fields is 12 in 2008 and is less than 12 in 2009 and 2010, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to: [('1/1/2008', '12/31/2008'),
  ('1/1/2009', '1/1/2009+ #months' no record is written if #months=0), ('1/1/2010', '1/1/2010+ #months' no record is written if #months=0)]
- if the value of the fields is 12 in 2009 and 2010 and is less than 12 in 2008, payer_plan_period_start_date is calculated by subtracting #months
  from '12/31/2008' and payer_plan_period_end_date is set to '12/31/2010'.
- if the value of the fields is 12 in 2009 and is less than 12 in 2008 and 2010, payer_plan_period_start_date is calculated by subtracting #months
  from 12/31/2008 and payer_plan_period_end_date is calculated by adding #months to '12/31/2009'
- if the value of the fields is 12 is 2010 and is less than 12 in 2008 and 2009, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date
  set to: [('1/1/2008', '1/1/2008+ #months' no record is written if #months=0),
  ('1/1/2009', '1/1/2009+ #months' no record is written if #months=0), ('1/1/2010', '12/31/2010')]
- if the value of the fields is less than 12 in 2008, 2009, and 2010, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to - [('1/1/2008', '1/1/2008+ #months' - no record is written if #months=0),
  ('1/1/2009', '1/1/2009+ #months' - no record is written if #months=0), ('1/1/2010', '1/1/2010+ #months' - no record is written if #months=0)]

k) Input files (DE_1-DE_20) have some ICD9/HCPCS/NDC codes that are
not defined in the concept file and therefore such records are not
processed by the program and are written to the
'unmapped_code_log.txt' file. This file is opened in append mode so
that if more than one input file is processed together, the program
should append unmapped codes from all input files instead of
overwriting. So the file 'unmapped_code_log.txt' must be deleted if
you want to rerun the program with the same input file. We list the
unmapped codes that occurred 50 or more times along with their
putative source vocabulary (ICD9/HCPCS/CPT4/NDC) below. Some appear to
be typos or invalid entries. Others may represent ICD9 codes that are
not part of ICD9CM. For instance the 04.xx codes are listed on some
non-US lists of ICD9 codes (see for example
[04.22](http://jgp.uhc.com.pl/doc/39.5/icd9/04.22.html)).

      Count Vocabulary  Code
      ----- ----------- ----
      54271 ICD9        XX000
      11697 HCPCS/CPT4  201E5
       5293 ICD9        0422
       5266 ICD9        0432
       5249 ICD9        0440
       5240 ICD9        0430
       5220 ICD9        0421
       5208 ICD9        0429
       5206 ICD9        0431
       5204 ICD9        0433
       5157 ICD9        0439
       5119 ICD9        0420
       2985 ICD9        OTHER
       1773 HCPCS/CPT4  0851
       1153 HCPCS/CPT4  99910
       1038 ICD9        30513
        993 ICD9        30512
        925 ICD9        30510
        897 ICD9        30511
        655 HCPCS/CPT4  90699
        406 HCPCS/CPT4  01
        327 HCPCS/CPT4  0841
        313 NDC         OTHER
        286 HCPCS/CPT4  0521
        270 HCPCS/CPT4  520
        234 HCPCS/CPT4  99998
        211 ICD9        9
        180 HCPCS/CPT4  00000
        125 ICD9        30040
        119 HCPCS/CPT4  XXXXX
        101 HCPCS/CPT4  J2000
         99 HCPCS/CPT4  A9170
         93 HCPCS/CPT4  X9999
         92 HCPCS/CPT4  A9160
         80 ICD9        72330
         71 HCPCS/CPT4  GOOO8
         71 HCPCS/CPT4  GO283
         68 ICD9        73930
         59 ICD9        4900
         54 HCPCS/CPT4  521
         50 ICD9        VO48
