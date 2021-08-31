# gmx_db

An application to run and manage simulations across multiple hosts. Build around postgrsql.


## Requirements

* Postgresql server and sufficient access rights to create a new database.

* Python 3.8+ 
    * Earlier versions _might_ work  but the recent updates to the subprocess module could cause issues

## Installation

`$gmx_db/bin/create_db.sh --dbname "mydb"`

For additional options see:

`$gmx_db/bin/create_db.sh -h`

## Concepts

gmx_db is build around two central concepts.

1) <ins>Job Status</ins>

    Jobs managed by gmx_db fall in two main categories:
<br/><br/>
    1) **Active jobs**  require some action to be taken, they are further divided in jobs that require single action
       (such as jobs waiting to be run/submitted) and jobs that require periodic action, such as jobs running on a queueing system or waiting for another job to finish.
    2) **Inactive jobs** have either finished successfully, are blocked, have failed, or had a dependency that failed.
<br/>    
    The central gmxdb process will periodically scan the status of active jobs, take action and if applicable update job status.
<br/><br/> 
2) <ins>Inheritance</ins>
   
   Jobs can be in a hierarchical relationship with one another i.e., the progression of job B 
   depends on the status of job A. Currently we only map single parent, directional relationships. If job **B** depends
   on job **A** it also has access to the output of job **A**.

## Usage

To use the database run `$gmxdb/bin/gmxdb.sh` on any host you want to run simulations on.
The process will periodically scan the database, update the status of running jobs and submit new jobs. 
Multiple gmxdb jobs can be active at the same time, provided they have access to the same file system.

New jobs can be submitted in one of two (three) ways:

1) Running `$gmx_db/bin/db_submit.sh` and specifying the job parameters (for a single job).
2) Running `$gmx_db/bin/db_submit.sh` and providing job instructions in a config file.
3) Modifying the database directly ( Probably not a good idea ).

Option 2 is the most useful and flexible. An example config file is provided in examples.

The configuration file is a json file containing a list of jobs, each specified in dictionary format.

Each job requires a number of key/value pairs:
    
    

    [ required ]
    
    "cmd" : The command to run, currently supported options are grompp, g_submit and shell.
            The las option will execute whatever is specified in "args".

    "args": A dictionary of key value pairs that make the arguments to the command. 
    
    [ optional ]
    
    "base": The directory the job is run from.

    "dependency": An integer specifying a parent job. A positive integer will be interpreted as a simulation id.
                  A negative integer will be interpreted as a preceding job in the configuration file,
                  with -1 indicating the job immediately prior to the current job.


The configuration file can contain shell variables (e.g. $PWD).
If a job depends on an earlier job, files from the parent job cna be specified using **%** followed by the id of the specific file.

## Limitations

The current version of gmx_db is limited in a number of ways:

1) Jobs submitted to a queue system are only supported in the form of g_submit,
 a program that is used at the  MPIBPC to automatically determine the optimal resources for a mdrun job. 

2) Only grompp and g_submit is supported natively. Any job can be run using the "cmd" keyword in a configuration file,
   but the (names) of the outfiles have to then be provided in advance. 