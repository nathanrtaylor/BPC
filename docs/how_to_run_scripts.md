1. Generate a new jobs_MMDDYY.yml file by duplicating an existing file
in the terminal, write out the following command with the filename you want to duplicate at the end:
    python .\src\update_jobs_yml.py configs/jobs_062526.yml

2. Run a compare between two jobs files to see if the cohort_ids incremented correctly
    Open the file \src\config_compare.py
    change the file names in the header to match your old and new jobs file, example:
        FILE_1 = "configs/jobs_062526.yml"
        FILE_2 = "configs/jobs_070126.yml"
    save the file
    in the terminal, write out the following command and hit enter
    python .\src\config_compare.py

3. Run the outlier pipeline and create the proposed cohort files
    Open the jobs file you want to run, example: jobs_070126.yml
    Make sure the output_dir is correct
    Save the file
    in the terminal, write out the following command and hit enter
        python -m src.runner configs/jobs_070126.yml
    Make sure the jobs file is the one you want to use
    Once it is complete it will display a count of the files created and the number of experts in each one. 