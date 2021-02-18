# Project-USAID-IBTCI
Data pipeline for USAID-IBTCI.

This pipeline fetches all project data from a Rapid Pro instance and Coda, and processes it to produce data for
manual labelling and verification in Coda, graphs summarising code distributions, and CSV files suitable for 
downstream analysis.

## Pre-requisites
Before the pipeline can be run, the following tools must be installed:
 - Docker
 - Bash
 
Development requires the following additional tools:
 - Python 3.6
 - pipenv
 - git

## Usage
A pipeline run consists of the following five steps, executed in sequence:
1. Download coded data from Coda.
2. Fetch all the relevant data from Rapid Pro.
3. Process the raw data to produce the outputs required for coding and then for analysis.
4. Upload the new data to Coda for manual verification and coding.
5. Generate analysis graphs (currently the number of messages/individuals per week and the seasonal distribution of 
   labels for each code scheme).
6. Back-up the project data root.
7. Upload execution logs/archives to long term storage.

To simplify the configuration and execution of these stages, this project includes a `run_scripts`
directory, which contains shell scripts for driving each of those stages. 

To run the entire pipeline, see [Run All Pipeline Stages](#run-all-pipeline-stages).

To run the above stages individually, see [these sections](#1-download-coded-data-from-coda)

### Run All Pipeline Stages
To run all the pipeline stages at once, and create a compressed backup of the data directory after the run,
run the following command from the `run_scripts` directory:

```
$ ./run_pipeline.sh <user> <pipeline-configuration-file-path> <coda-pull-auth-file> <coda-push-auth-file> <avf-bucket-credentials-path> <coda-tools-root> <data-root> <data-backup-dir> <performance-logs-dir>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org` 
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `coda-pull-auth-file` is an absolute path to the private credentials file for the Coda instance to download manually coded datasets from.
- `coda-push-auth-file` is an absolute path to the private credentials file for the Coda instance to upload datasets to be manually coded to.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `coda-tools-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
- `data-backup-dir` is a directory which the `data-root` directory will be backed-up to after the rest of the
  pipeline stages have completed. The data is gzipped and given the name `data-<utc-date-time-now>-<git-HEAD-hash>`

### 1. Download Coded Data from Coda
This stage downloads coded datasets for this project from Coda (and is optional if manual coding hasn't started yet).
To use, run the following command from the `run_scripts` directory: 

```
$ ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>
```

where:
- `coda-auth-file` is an absolute path to the private credentials file for the Coda instance to download coded datasets from.
- `coda-v2-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Downloaded Coda files are saved to `<data-root>/Coded Coda Files/<dataset>.json`.

### 2. Fetch Raw Data
This stage fetches all the raw data required by the pipeline from Rapid Pro.
To use, run the following command from the `run_scripts` directory:

```
$ ./2_fetch_raw_data.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org` 
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Raw data will be saved to TracedData JSON files in `<data-root>/Raw Data`.

### 3. Generate Outputs
This stage processes the raw data to produce outputs for ICR, Coda, and messages/individuals/production
CSVs for final analysis.
To use, run the following command from the `run_scripts` directory:

```
$ ./3_generate_outputs.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org`.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  All output files will be saved in `<data-root>/Outputs`.
   
As well as uploading the messages, individuals, and production CSVs to Drive (if configured in the 
pipeline configuration json file), this stage outputs the following files to `<data-root>/Outputs`:
 - Local copies of the messages, individuals, and production CSVs (`messages.csv`, `individuals.csv`, 
   `production.csv`)
 - A serialized export of the list of TracedData objects representing all the data that was exported for analysis 
   (`messages_traced_data.json` for `messages.csv` and `individuals_traced_data.json` for `individuals.csv`)
 - For each week of radio shows, a random sample of 200 messages that weren't classified as noise, for use in ICR (`ICR/`)
 - Coda V2 messages files for each dataset (`Coda Files/<dataset>.json`). To upload these to Coda, see the next step.

### 4. Upload Auto-Coded Data to Coda
This stage uploads messages to Coda for manual coding and verification.
Messages which have already been uploaded will not be added again or overwritten.
To use, run the following command from the `run_scripts` directory:

```
$ ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>
```

where:
- `coda-auth-file` is an absolute path to the private credentials file for the Coda instance to download coded datasets from.
- `coda-v2-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Downloaded Coda files are saved to `<data-root>/Coded Coda Files/<dataset>.json`.
  
### 5. Generate Analysis Graphs
This stage generates graphs of traffic per week, and of the seasonal distribution of codes for each code scheme.
To use, run the following command from the `run_scripts` directory:

```
$ ./5_generate_analysis_graphs.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org`.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  All output files will be saved in `<data-root>/Outputs`.


### 6. Back-up the Data Directory
This stage makes a backup of the project data directory by creating a compressed, versioned, time-stamped copy at the
requested location.
To use, run the following command from the `run_scripts` directory:

```
$ ./6_backup_data_root.sh <data-root> <data-backups-dir>
```

where:
- `data-root` is an absolute path to the directory to back-up.
- `data-backups-dir` is a directory which the `data-root` directory will be backed-up to.
  The data is gzipped and given the name `data-<utc-date-time-now>-<git-HEAD-hash>`.


### 7. Upload logs
This stage uploads the archive produced in the last step and a memory profile log of the generate outputs stage
to Google Cloud storage.
To use, run the following command from the `run_scripts` directory:

```
$ ./7_upload_logs.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <memory-profile-file-path> <data-archive-file-path>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org`.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `run-id` is a unique identifier for the run being uploaded. This will be included in all of the uploaded file names.
- `memory-profile-file-path` is the path to the memory profile log file for this run to upload.
- `data-archive-file-path` is the path to the gzipped data archive produced by step (6) to upload.

## Development

### CPU Profiling
To run the main processing stage with statistical cpu profiling enabled, pass the argument 
`--profile-cpu <cpu-profile-output-file>` to `run_scripts/3_generate_outputs.sh`.
The output is a HTML file generated by the statistical profiler [pyinstrument](https://github.com/joerick/pyinstrument),
which can be explored in a web browser.

### Memory Profiling
To run the main processing stage with memory profiling enabled, pass the argument
`--profile-memory <memory-profile-output-file>` to `run_scripts/3_generate_outputs.sh`.
The output file lists (memory usage, timestamp) pairs, sampled approximately every 0.1s.

To plot a graph of memory usage over time, on the host machine run:
```
# Install dependencies
$ pip install -U memory_profiler
$ pip install matplotlib

# Plot graph
$ mprof plot <path-to-memory-profile-file>
```

To annotate the graph with when specific functions were entered and exited, add `@profile` to 
the functions of interest. There is no need to import anything.

For full details on the memory profiler, see its [documentation page](https://pypi.org/project/memory-profiler/).

### Configuration JSON Spec

```
{
  "PipelineName": string,              // Name of this pipeline, for use in logging, coding_plans etc.
  "RawDataSources": [                  // Configuration for where to fetch raw messages or comments from. 
    {
      "SourceType": "RapidPro",        // Configure download of runs from a Rapid Pro workspace.
      "Domain": string,                // Domain of Rapid Pro workspace to download data from e.g. rapidpro.io.
      "TokenFileURL": string,          // GS URL to a text file containing the authorisation token for the Rapid Pro workspace.
      "ContactsFileName": string       // TODO: Remove this
      "ActivationFlowNames": string[], // The names of the Rapid Pro flows that contain the radio show responses.
      "SurveyFlowNames": string[],     // The names of the Rapid Pro flows that contain the survey responses.
      "TestContactUUIDs": string[]     // Rapid Pro contact UUIDs of test contacts. Runs for any of those test contacts will be tagged with {'test_run': True}, and dropped when the pipeline is run with "FilterTestMessages" set to true..
    } | {
      "SourceType": "GCloudBucket",    // Configure download of de-identified data directly from a Google Cloud Bucket. Data is downloaded directly, with no further processing applied.
      "ActivationFlowURLs": string[],  // GS URLs to download radio show response runs data from. 
      "SurveyFlowURLs": string[]       // GS URLs to download survey response runs data from.
    } | {
      "SourceType": "RecoveryCSV",     // Configure download of de-identified data from a recovery CSV in a Google Cloud Bucket. Data is downloaded and converted to TracedData. The recovery CSV must have headers "Sender", "Message", and "ReceivedOn".
      "ActivationFlowURLs": string[],  // GS URLs to download recovery CSVs to process as activation data from. 
      "SurveyFlowURLs": string[]       // GS URLs to download recoveny CSVs to process as survey data from.
    } | {
      "SourceType": "Facebook",        // Configure download of comments from Facebook.
      "PageID": string,                // ID of page to download comments from.
      "TokenFileURL": string,          // GS URL to a text file containing the page authorisation token.
      "Datasets": [
        {
          "Name": string,              // The name to give this dataset e.g. facebook_e01
          "PostIDs"?: string[],        // Optional unless 'Search' is not provided. A list of posts to download comments from, both top-level and replies to other comments.
          "Search"?: {                 // Optional unless "PostIDs" is not provided.
            "Match": string,           // String to look for in posts e.g. a hashtag. Comments on all posts that contain this string in the time-range will be downloaded.
            "StartDate": ISO string,   // Start date of time range to search, inclusive. Comments on posts created within the time-range and containing the "Match" string will be downloaded. 
            "EndDate": ISO string      // End date of time range to search, exclusive.
          }
        }
      ]
    }
  ],
  "UUIDTable": {                       // Configuration for the Firestore phone number/app-scoped facebook id <-> uuid table.
    "FirebaseCredentialsFileURL": string // GS URL to a json file containing the Firebase credentials for the UUID table.
    "TableName": string                  // Name of the table in the Firestore to store the id <-> uuid mappings.
    "UuidPrefix": string                 // Prefix to include in each generated uuid e.g. 'avf-phone-uuid-'.
  },
  "TimestampRemappings"?: [            // Configuration for remapping activation messages received within the given time range to another field. Use these to bulk correct messages received by the wrong activation flow.
    {
      "TimeKey": string                   // SourceKey containing the time field to remap.
      "ShowPipelineKeyToRemapTo": string  // PipelineKey to remap the raw messages to.
      "RangeStartInclusive": ISO string   // Start of time range to remap, inclusive.
      "RangeEndExclusive": ISO string     // End of time range to remap, exclusive.
      "TimeToAdjustTo"?: ISO string       // Datetime to readjust each message object's `time_key` to. If not provided, timestamps will not be adjusted.
    }
  ],
  "SourceKeyRemappings": [             // Configuration to convert the keys used by the raw data sources into terminology to be used by the rest of the pipeline.  
    {
      "SourceKey": string              // Name of key to remap in the raw data.
      "PipelineKey": string,           // Name of the key to remap to.
      "IsActivationMessage"?: bool     // Whether the key being remapped contains raw activation messages. Defaults to false.
    }
  ],
  "ProjectStartDate": ISO string       // Start date for the project. Activation messages received before this date are dropped by the pipeline. Survey messages are not filtered, to allow for demogs from previous seasons to be easily reused.
  "ProjectEndDate": ISO string         // End date for the project. Activation messages received after this date are dropped by the pipeline. Survey messages are not filtered.
  "FilterTestMessages": bool           // Whether to filter out messages from contacts with uuids in RawDataSources.RapidProSource.TestContactUUIDs above.
  "MoveWSMessages": bool               // Whether to perform Wrong-Scheme correction.
  "AutomatedAnalysis": {
    "GenerateRegionThemeDistributionMaps": bool,   // Whether to export a region map for each rqa theme. If False, only exports a region map showing the total participants. 
    "GenerateDistrictThemeDistributionMaps": bool, // Whether to export a district map for each rqa theme. If False, only exports a district map showing the total participants.
    "GenerateMogadishuDistributionMaps": bool,     // Whether to export a Mogadishu sub-district map for each rqa theme. If False, only exports a Mogadishu sub-district map showing the total participants.
    "TrafficLabels"?: [                // Configuration for the traffic_analysis. Activation messages received within each time range are counted and exported to a CSV. If not provided, no traffic_analysis CSV is produced.
      {
        "StartDate": ISO string,       // Start date for the time range to search, inclusive.
        "EndDate": ISO string,         // End date for the time range to search, exclusive.
        "Label": string                // Label to assign to messages received within this time range.
      }
    ]
  }
  "DriveUpload"?: {                    // Configuration for uploading outputs to Google Drive. If no configuration is provided, Drive upload will be skipped.
    "DriveCredentialsFileURL": string, // GS URL to a Google Drive service account credentials file.
    "ProductionUploadPath": string,    // Path in the service account's 'shared_with_me' folder to upload the production CSV file to.
    "MessagesUploadPath": string,      // Path in the service account's 'shared_with_me' folder to upload the messages CSV file to.
    "IndividualsUploadPath": string,   // Path in the service account's 'shared_with_me' folder to upload the individuals CSV file to.
    "AutomatedAnalysisDir": string     // Path in the service account's 'shared_with_me' folder to upload the automated analysis directory to.
  },
  "MemoryProfileUploadBucket": string, // The GS bucket name to upload the memory profile logs to. The name will be appended with the "BucketDirPath" and the file basename to generate the archive upload location.
  "DataArchiveUploadBucket": string,   // The GS bucket name to upload the data archives to. The name will be appended with the "BucketDirPath" and the file basename to generate the archive upload location.
  "BucketDirPath": string              // The GS bucket folder path to store the data archive & memory log files to.
```
