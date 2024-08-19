# SPS Commerce API ETL - Fix Request

## Overview

This repository contains the ETL (Extract, Transform, Load) process that integrates with the SPS Commerce API. The ETL process extracts data from the SPS API, performs necessary transformations, and loads it into our internal systems. We have encountered an issue within this ETL process that requires assistance from the SPS Commerce Dev Team.

## Purpose

This README serves as a guide for the SPS Commerce Dev Team to review the existing ETL code, understand the problem, and suggest or implement a fix.

## Problem Statement

### Issue Description

We are experiencing several issues with our ETL process when communicating with the SPS Commerce API. The specific errors are as follows:

1. **Error 1**
   - **Endpoint**: `GET /transactions/v5/data/out/`
   - **Params**: 
     ```json
     {'limit' : 1000}
     ```
   - **Headers**:
     ```json
     {'Authorization': "Bearer " + self.ACCESS_TOKEN}
     ```
   - **Response**:
     ```json
     {'status': 'error', 'error': {'error': 'notFound', 'errorDescription': "Directory '/out/' not found."}}
     ```

2. **Error 2**
   - **Endpoint**: `GET /transactions/v5/data/out/`
   - **Params**: 
     ```json
     {'limit' : 1000}
     ```
   - **Headers**:
     ```json
     {'Authorization': "Bearer " + self.ACCESS_TOKEN}
     ```
   - **Response**:
     ```json
     {'status': 'error', 'error': {'error': 'notFound', 'errorDescription': "Directory '/in/' not found."}}
     ```

3. **Error 3**
   - **Endpoint**: `GET /v1/forms`
   - **Params**: 
     ```json
     {}
     ```
   - **Headers**:
     ```json
     {'Authorization': "Bearer " + self.ACCESS_TOKEN, 'Content-Type': 'application/json'}
     ```
   - **Response**:
     ```json
     {'error': {'error': 'requestError', 'errorDescription': 'The requested host or path is either temporarily unavailable or not found.'}, 'status': 'error'}
     ```

### Symptoms

- Intermittent failures in data extraction.
- Specific directories (`/out/` and `/in/`) cannot be found as per the error messages.
- Failure in retrieving forms due to a "requestError".

### Impact

- Delays in data processing.
- Incomplete or inaccurate data being loaded into the internal system.
- Disruption of automated workflows dependent on these API endpoints.

## Code Structure

### `ETLs/etl_spscommerce.py`
- This script contains the class of the ETL, with dedicated methods to extract data from the SPS Commerce API.

### `main/sps_main.py`
- Handles API requests to the SPS Commerce API, including authentication, data retrieval, and error handling. This code contains the actual use of the ETL class.

## Steps to Reproduce

1. Execute the ETL process by running the `main/sps_main.py` script.
2. Ensure the correct `ACCESS_TOKEN` is used for API requests. Detailed steps to get an `ACCESS_TOKEN` are explained in `SPScommerce.ipynb`
3. Observe the errors generated during API calls to the specified endpoints.

## Expected Behavior

- The ETL process should successfully extract data from the SPS Commerce API, transform it according to business requirements, and load it into the internal system without errors.

## Actual Behavior

- The ETL process encounters errors related to missing directories or paths and fails to retrieve the necessary data.
- The script should output 
```
Retrieve SPScommerce credentials
SPScommerce credentials retrieved
Token is invalid, refreshing token
Getting transactions paths
{'path': '/testin/', 'type': 'directory', 'url': 'https://api.spscommerce.com/transactions/v5/data/testin/'}
{'path': '/testout/', 'type': 'directory', 'url': 'https://api.spscommerce.com/transactions/v5/data/testout/'}
Transactions paths: 87 retrieved
Getting transactions paths
Error getting transactions
Error type :404
{'status': 'error', 'error': {'error': 'notFound', 'errorDescription': "Directory '/out/' not found."}}
```
## Requested Assistance

We are seeking the following help from the SPS Commerce Dev Team:

1. **Review of ETL Code**: Analyze the current implementation to identify the root cause of the issue.
2. **Fix or Guidance**: Provide a fix for the issue or guidance on how to resolve it.
3. **Best Practices**: Recommend any best practices for interacting with the SPS Commerce API that might prevent similar issues in the future.

## Contact

Please reach out to the following contact for any further information or clarification:

- **Name**: Pierre DENIG
- **Email**: pierre@datagem-consulting.com

Thank you for your assistance!
